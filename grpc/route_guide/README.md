##### code official reference: [https://github.com/grpc/grpc/tree/master/examples/python/route_guide](https://github.com/grpc/grpc/tree/master/examples/python/route_guide)
1. Generate python code
```
python -m grpc_tools.protoc  -I ../protos --python_out=. --grpc_python_out=. ../protos/helloworld.proto
```
2. Run demos
```
# server part
python route_guide_server.py
# client part
python route_guide_client.py
```
