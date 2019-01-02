import argparse
import cv2
import mxnet as mx
import numpy as np
from collections import namedtuple
from sklearn.metrics import average_precision_score
from sklearn.preprocessing import label_binarize
import time


def convert_image(img):
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    img = np.float32(img)
    img = np.swapaxes(img, 0, 2)
    img = np.swapaxes(img, 1, 2)
    img = img[np.newaxis, :]
    #img = (img - 127.5) / 128
    img[:, 0, :, :] -= 123.68
    img[:, 1, :, :] -= 116.779
    img[:, 2, :, :] -= 103.939
    return img


def get_labels(rec_file):
    labels = []
    rec_reader = mx.recordio.MXRecordIO(args.test_data, "r")
    while True:
        item = rec_reader.read()
        if not item:
            break
        header, _ = mx.recordio.unpack_img(item)
        labels.append(header.label)
    rec_reader.close()
    return labels


def predict(rec_file, model_prefix, load_epoch, image_shape, gpu, num_classes, 
            step=False, batch_size=32, rgb_mean=[123.68,116.779,103.939]):
    start = time.time()
    ctx = mx.gpu(gpu) if gpu is not None else mx.cpu()
    if not step:
        dataiter = mx.io.ImageRecordIter(
            path_imgrec=rec_file,
            rand_crop=False,
            rand_mirror=False,
            data_shape=(3, image_shape[0], image_shape[1]),
            batch_size=batch_size,
            shuffle=False,
            mean_r=rgb_mean[0],
            mean_g=rgb_mean[1],
            mean_b=rgb_mean[2])
        model = mx.model.FeedForward.load(model_prefix, load_epoch, ctx=ctx)
        predicts = model.predict(dataiter)
        end = time.time()
        print(end-start)
        return predicts
    else:
        sym, arg_params, aux_params = mx.model.load_checkpoint(model_prefix, load_epoch)
        model = mx.mod.Module(symbol=sym, context=ctx, label_names=None)
        model.bind(for_training=False, data_shapes=[('data', (1, 3, image_shape[0], image_shape[1]))])
        model.set_params(arg_params, aux_params, allow_missing=True)
        predicts = []
        rec_reader = mx.recordio.MXRecordIO(args.test_data, "r")
        idx = 0
        while True:
            item = rec_reader.read()
            if not item:
                break
            _, img = mx.recordio.unpack_img(item)
            img = cv2.resize(img, (image_shape[0], image_shape[1]))
            img = convert_image(img)
            Batch = namedtuple('Batch', ['data'])
            model.forward(Batch([mx.nd.array(img)]))
            prob = model.get_outputs()[0].asnumpy()
            prob = np.squeeze(prob)
            predicts.append(prob)
        rec_reader.close()
        end = time.time()
        print(end-start)
        return predicts


def ap(y_test, y_score, num_classes):
    average_precision = dict()
    for i in range(num_classes):
        average_precision[i] = average_precision_score(y_test[:, i], y_score[:, i])
    average_precision["micro"] = average_precision_score(y_test, y_score, average="micro")
    return average_precision


def evaluate(args):
    labels = get_labels(args.test_data)
    labels = label_binarize(labels, classes=[i for i in range(args.num_classes)])
    image_shape = [ int(it) for it in args.image_shape.split(",") ]
    predicts = predict(args.test_data, args.model_prefix, args.load_epoch, image_shape=image_shape, gpu=args.gpu, num_classes=args.num_classes, step=False)
    predicts = np.array(predicts)
    average_precision = ap(labels, predicts, args.num_classes)
    return average_precision


def arg_parser():
    parser = argparse.ArgumentParser(description='Image classification')
    parser.add_argument("--gpu", type=int, default=None, help="gpu id")
    parser.add_argument("--test-data", type=str, help="test data rec file")
    parser.add_argument("--model-prefix", type=str, required=True, help="attribute model prefix")
    parser.add_argument("--load-epoch", type=int, required=False, default=0, help="load specified epoch model params")
    parser.add_argument("--image-shape", type=str, required=False, default="32,32", help="")
    parser.add_argument("--num-classes", type=int, required=False, default=10, help="")
    parser.add_argument("--batch-size", type=int, required=False, default=32, help="")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = arg_parser()
    res = evaluate(args)
    print(res)
    #predict_rec(args)  
    #ap = evaluate(args) 
