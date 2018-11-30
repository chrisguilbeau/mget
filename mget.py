#!/usr/bin/env python3
'''
Download a file in parts over http.

Usage: {} [options] url

-o <string>
    Write file contents to <string> instead of the file's actual name

-c <integer>
    Use <integer> bytes for the chunk size (default is {})

-m <integer>
    Download <integer> chunks (default is {})

-f
    Force writing file even if it exists

-p <integer>
    Use <integer> concurrent connections (default is {})
'''

from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from getopt             import getopt
from http               import client
from os.path            import isfile
from sys                import argv
from sys                import exit
from sys                import stdout
from urllib.parse       import urlparse

# default values
CHUNK_SIZE = 1048576 # https://en.wikipedia.org/wiki/Mebibyte
MAX_CHUNKS = 4
DOP = 1

def get_cnn(netloc):
    '''
    Return an http connection object
    '''
    return client.HTTPConnection(netloc)

def get_head_resp(urlParts):
    '''
    Return head response for the desired file
    '''
    try:
        cnn = get_cnn(urlParts.netloc)
        cnn.request('HEAD', urlParts.path)
        resp = cnn.getresponse()
        resp.read()
        return resp
    finally:
        cnn.close()

def get_size(urlParts):
    '''
    Return size of the desired file
    '''
    resp = get_head_resp(urlParts)
    if resp.status != 200:
        print("File doesn't exist on server")
        exit(1)
    return int(resp.getheader('Content-length'))

def is_range_supported(urlParts):
    '''
    Return if the range header is supported by the server. If a request
    comes back with 200 it means that chunking is not supported
    '''
    cnn = get_cnn(urlParts.netloc)
    cnn.request('GET', urlParts.path, headers={'Range': f'bytes={0-1}'})
    resp = cnn.getresponse()
    return resp.status == 206

def get_chunk(args):
    '''
    Fetch chunk, write to file and return the filename.
    '''
    urlParts, start, end, filename = args
    print('.', end='')
    stdout.flush()
    try:
        cnn = get_cnn(urlParts.netloc)
        cnn.request('GET', urlParts.path,
            headers={'Range': f'bytes={start}-{end}'})
        resp = cnn.getresponse()
        content = resp.read()
        return filename, content
    finally:
        cnn.close()

def write_chunks(urlParts, chunk_size, max_chunks, dop):
    '''
    Asynchronously fetch file in chunks and write them to disk,
    return a list of files that have been written
    '''
    size = get_size(urlParts)
    def get_chunk_args(i):
        return (urlParts, i, min(i + chunk_size - 1, size), i)
    # for performance sake, only calculate the arguments for the
    # chunks you need
    max_size = (min(size, chunk_size * max_chunks + 10))
    chunk_args = [get_chunk_args(i)
        for i in range(0, max_size, chunk_size)][:max_chunks]
    results = []
    with ThreadPoolExecutor(max_workers=dop) as pool:
        ftr = {pool.submit(get_chunk, arg): arg for arg in chunk_args}
        for f in as_completed(ftr):
            results.append(f.result())
    return results

def mget(url, chunk_size, filename, max_chunks, force, dop):
    '''
    Get a file referenced by a url in parts using chunk_size and write
    to disk using filename
    '''
    urlParts = urlparse(url)
    # check the file
    filename = filename or urlParts.path.split('/')[-1]
    if not force and isfile(filename):
        print(f'{filename} already exists')
        exit(1)
    if not is_range_supported(urlParts):
        print(f'Server does not support Range header')
        exit(1)
    parts = write_chunks(urlParts, chunk_size, max_chunks, dop)
    # write the file before the main reconstruction loop in case the
    # file you are fetching is empty
    with open(filename, 'wb') as f:
        pass
    for i, part in sorted(parts):
        with open(filename, 'ab') as f:
            f.write(part)
    print('done.')

if __name__ == '__main__':
    try:
        opts, args = getopt(argv[1:], 'o:p:c:m:f')
        assert len(args) == 1, 'Must provide url'
        url, = args
        chunk_size = CHUNK_SIZE
        max_chunks = MAX_CHUNKS
        filename = None
        force = False
        dop = 1
        for o, a in opts:
            if o == '-o':
                filename = a
            elif o == '-c':
                chunk_size = int(a)
            elif o == '-m':
                max_chunks = int(a)
            elif o == '-f':
                force = True
            elif o == '-p':
                dop = int(a)
    except Exception as e:
        print(str(e))
        print(__doc__.format(argv[0], CHUNK_SIZE, MAX_CHUNKS, DOP))
        exit(1)
    mget(url=url, chunk_size=chunk_size, filename=filename,
        max_chunks=max_chunks, force=force, dop=dop)
