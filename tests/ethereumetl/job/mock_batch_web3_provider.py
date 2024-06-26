# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import json

from tests.ethereumetl.job.mock_web3_provider import (
    MockWeb3OrWeb3Provider,
    MockWeb3Provider,
    build_file_name,
)


class MockBatchWeb3Provider(MockWeb3Provider):
    def __init__(self, read_resource):
        super().__init__(read_resource)
        self.read_resource = read_resource

    def make_batch_request(self, text):
        batch = json.loads(text)
        web3_response = []
        for req in batch:
            method = req['method']
            params = req['params']
            file_name = build_file_name(method, params)
            file_content = self.read_resource(file_name)
            response = json.loads(file_content)
            response['id'] = req['id']
            web3_response.append(response)
        return web3_response


class MockBatchWeb3OrWeb3Provider(MockWeb3OrWeb3Provider):
    def __init__(self, read_resource, write_resource, web3):
        super().__init__(read_resource, write_resource, web3)
        self.read_resource = read_resource
        self.write_resource = write_resource
        self.web3 = web3

    def make_batch_request(self, text):
        batch = json.loads(text)
        assert isinstance(batch, list)
        web3_response = []
        for req in batch:
            method = req['method']
            params = req['params']
            file_name = build_file_name(method, params)
            print(f'{type(self).__name__}: reading {file_name=}')
            try:
                file_content = self.read_resource(file_name)
                response = json.loads(file_content)
            except ValueError:
                response = self.web3.make_request(method, params)
                file_content = json.dumps(response)
                print(f'Warning: {file_name} not found, using real web3 response')
                self.write_resource(file_name, file_content)
                print(f'Saved real web3 response to {file_name}')
            response['id'] = req['id']
            web3_response.append(response)
        assert len(batch) == len(web3_response)
        assert {req['id'] for req in batch} == {res['id'] for res in web3_response}
        return web3_response
