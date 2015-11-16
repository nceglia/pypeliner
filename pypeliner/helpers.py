import os
import logging
import stat
import hashlib
import warnings
import errno

def abspath(path):
    if path.endswith('/'):
        return os.path.abspath(path) + '/'
    else:
        return os.path.abspath(path)

class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        header = logging.Formatter.format(self, record)
        return header + record.message.rstrip('\n').replace('\n', '\n\t')

def which(name):
    if os.environ.get('PATH', None) is not None:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            p = os.path.join(p, name)
            if os.access(p, os.X_OK):
                return p
    raise Exception('unable to find ' + name + ' in the system path')

def set_executable(filename):
    mode = os.stat(filename).st_mode
    mode |= stat.S_IXUSR
    os.chmod(filename, stat.S_IMODE(mode))

def md5_file(filename, block_size=8192):
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(block_size), b''): 
             md5.update(chunk)
    return md5.digest()

def overwrite_if_different(new_filename, existing_filename):
    do_copy = True
    try:
        do_copy = md5_file(existing_filename) != md5_file(new_filename)
    except IOError:
        pass
    if do_copy:
        os.rename(new_filename, existing_filename)

def makedirs(dirname):
    dirname = abspath(dirname)
    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno != 17:
            raise
    assert os.path.isdir(dirname)

def symlink(source, link_name):
    source = os.path.abspath(source)
    try:
        os.remove(link_name)
    except OSError as e:
        if e.errno != 2:
            raise
    os.symlink(source, link_name)

class DirectoryLock(object):
    def __init__(self):
        self.locked_directories = list()
    def add_lock(self, lock_directory):
        try:
            makedirs(os.path.join(lock_directory, os.path.pardir))
            os.mkdir(lock_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise Exception('Pipeline already running, remove {0} to override'.format(lock_directory))
            else:
                raise
        self.locked_directories.append(lock_directory)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        for lock_directory in self.locked_directories:
            try:
                os.rmdir(lock_directory)
            except:
                warnings.warn('unable to unlock ' + lock_directory)


