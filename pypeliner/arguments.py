import os

import pypeliner.helpers
import pypeliner.resources
import pypeliner.identifiers


def _max_subseq(seq1, seq2):
    common = 0
    for a, b in zip(seq1, seq2):
        if a != b:
            break
        common += 1
    return common


class Arg(object):
    def __init__(self, job, name, axes, **kwargs):
        self.name = name
        self.kwargs = kwargs
        self.direct_write = job.direct_write

        # Match managed argument axes to job axes
        if axes is None:
            self.node = job.node
            self.axes = ()
        else:
            common = _max_subseq(axes, job.node.axes)
            self.node = job.node[:common]
            self.axes = axes[common:]

        # Nodes on which to split / merge if any
        self.arg_nodes = list(job.db.nodemgr.retrieve_nodes(self.axes, self.node))
        self.is_split_merge = (len(self.axes) > 0)

        # Create resources referenced by this argument
        self.resources = []
        for node in self.arg_nodes:
            self.resources.append(self.create_resource(job.db, node))

        # Check if an arg possibly originates an axis, only used for outputs
        self.matches_origin = False
        for origin in job.job_def.origins:
            if _is_subseq(origin, axes):
                self.matches_origin = True

    def get_node_chunks(self, node):
        chunks = tuple([a[1] for a in node[-len(self.axes):]])
        if len(chunks) == 1:
            return chunks[0]
        return chunks

    @property
    def id(self):
        return (self.name, self.node, self.axes)
    def get_inputs(self):
        return []
    def get_outputs(self):
        return []
    def get_split_chunks(self):
        return None
    def resolve(self):
        return None
    def updatedb(self, db):
        pass
    def finalize(self):
        pass

    def allocate(self):
        for resource in self.resources:
            resource.allocate()

    def pull(self):
        for resource in self.resources:
            resource.pull()

    def allocate(self):
        pass
    def pull(self):
        pass
    def push(self):
        pass


class TemplateArg(Arg):
    """ Templated name argument

    The name parameter is treated as a string with named formatting.  Resolves to the name formatted using the node
    dictionary.

    """
    def setup_resources(self, db):
        if self.kwargs.get('template') is not None:
            self.filename = self.kwargs['template'].format(**dict(self.node))
        else:
            self.filename = self.name.format(**dict(self.node))
    def resolve(self):
        return self.filename


class TempSpaceArg(Arg):
    """ Temporary space argument

    Resolves to a filename/directory contained within the temporary files directory.

    """
    def setup_resources(self, db):
        self.cleanup = self.kwargs.get('cleanup')
        self.filename = db.get_temp_filename(name, node)
    def get_outputs(self):
        yield pypeliner.resources.Dependency(self.name, self.node)
    def resolve(self):
        if self.node.undefined:
            raise Exception('Undefined node {} for {}'.format(self.node.displayname, self.name))
        return self.filename
    def pull(self):
        if self.cleanup in ('before', 'both'):
            pypeliner.helpers.removefiledir(self.filename)
        pypeliner.helpers.makedirs(os.path.dirname(self.filename))
    def push(self):
        if self.cleanup in ('after', 'both'):
            pypeliner.helpers.removefiledir(self.filename)


class MergeTemplateArg(Arg):
    """ Temp input files merged along a single axis

    The name parameter is treated as a string with named formatting.  Resolves to a dictionary with the keys as chunks
    for the merge axis.  Each value is the name formatted using the merge node dictionary.

    """
    def setup_resources(self, db):
        self.formatted = {}
        for node in self.arg_nodes:
            if self.kwargs.get('template') is not None:
                self.formatted[node[-1][1]] = self.kwargs['template'].format(**dict(node))
            else:
                self.formatted[node[-1][1]] = self.name.format(**dict(node))
    def resolve(self):
        return self.formatted


class FileArgMixin(object):
    def create_resource(self, db, node):
        filename = db.get_user_filename(self.name, node,
            fnames=self.kwargs.get('fnames'),
            template=self.kwargs.get('template'))
        resource = pypeliner.resources.UserResource(db.file_storage, self.name, node, filename,
            direct_write=self.direct_write,
            extensions=self.kwargs.get('extensions'))
        return resource


class InputFileArg(Arg, FileArgMixin):
    """ Input file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary.

    """
    def get_inputs(self):
        return self.resources
    def resolve(self):
        return self.resources[0].filename


class MergeInputFileArg(Arg, FileArgMixin):
    """ Input file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary.

    """
    def get_inputs(self):
        return self.resources
    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved


class OutputFileArg(Arg, FileArgMixin):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """


class SplitOutputFileArg(Arg, FileArgMixin):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """


class SplitOriginOutputFileArg(Arg, FileArgMixin):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """
    def __init__(self, job, name, axes, **kwargs):
        super(OutputFileArg, self).__init__(job, name, axes, **kwargs)
        filename_creator = db.get_user_filename_creator(
            self.name, self.node.axes + self.axes, fnames=self.fnames, template=self.template)
        self.filename_callback = UserFilenameCallback(
            db.file_storage, self.name, self.node, self.axes, filename_creator, **kwargs)
    def resolve(self):
        return self.filename_callback
    def allocate(self):
        pass
    def push(self):
        for resource in self.filename_callback.resources.itervalues():
            resource.push()
    def pull(self):
        pass


class OutputFileArg(Arg, FileArgMixin):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """
    def __init__(self, job, name, axes, **kwargs):
        super(OutputFileArg, self).__init__(job, name, axes, **kwargs)
        if self.matches_origin:
            filename_creator = db.get_user_filename_creator(
                self.name, self.node.axes + self.axes, fnames=self.fnames, template=self.template)
            self.filename_callback = UserFilenameCallback(
                db.file_storage, self.name, self.node, self.axes, filename_creator, **kwargs)
        else:
            self.filenames = dict()
            for resource in self.resources:
                self.filenames[self.get_node_chunks(resource.node)] = resource.write_filename
    def get_outputs(self):
        return self.resources
    def resolve(self):
        if self.matches_origin and self.is_split_merge:
            return self.filename_callback
        elif not self.matches_origin and self.is_split_merge:
            return self.filenames
        else:
            return self.filenames.values()[0]
    def allocate(self):
        if not self.matches_origin:
            for resource in self.resources:
                resource.allocate()
    def push(self):
        if self.matches_origin:
            for resource in self.filename_callback.resources.itervalues():
                resource.push()
        else:
            for resource in self.resources:
                resource.push()


class ObjArgMixin(object):
    def create_resource(self, db, node):
        filename = db.get_temp_filename(self.name, node)
        resource = pypeliner.resources.TempObjManager(db.obj_storage, self.name, node, filename)
        return resource


class TempInputObjArg(Arg, ObjArgMixin):
    """ Temporary input object argument

    Resolves to an object.  If func is given, resolves to the return value of func called with object as the only
    parameter.

    """
    def get_inputs(self):
        yield self.resource.input
    def resolve(self):
        self.obj = self.resource.get_obj()
        if self.kwargs.get('func') is not None:
            self.obj = self.kwargs['func'](self.obj)
        return self.obj


class TempMergeObjArg(Arg, ObjArgMixin):
    """ Temp input object arguments merged along single axis

    Resolves to an dictionary of objects with keys given by the merge axis chunks.

    """
    def get_inputs(self):
        for resource in self.resources:
            yield resource.input
    def resolve(self):
        self.resolved = dict()
        for resource in self.resources:
            obj = resource.get_obj()
            if self.kwargs.get('func') is not None:
                obj = self.kwargs['func'](obj)
            self.resolved[self.get_node_chunks(resource.node)] = obj
        return self.resolved


class TempOutputObjArg(Arg, ObjArgMixin):
    """ Temporary output object argument

    Stores an object created by a job.

    """
    def get_outputs(self):
        yield self.resource.output
    def resolve(self):
        return self
    def updatedb(self, db):
        self.resource.finalize(self.value)


class TempSplitObjArg(Arg, ObjArgMixin):
    """ Temporary output object arguments from a split

    Stores a dictionary of objects created by a job.  The keys of the dictionary are taken as the chunks for the given
    split axis.

    """
    def get_outputs(self):
        for resource in self.resources:
            yield resource.output
    def resolve(self):
        return self
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value.keys(), subset=self.axes_origin)
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            node_chunks = self.get_node_chunks(node)
            if node_chunks not in self.value:
                raise ValueError('unable to extract ' + str(node) + ' from ' + self.name + ' with values ' + str(self.value))
            instance_value = self.value[node_chunks]
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempObjManager(db.obj_storage, self.name, node, filename)
            resource.finalize(instance_value)


class TempFileArgMixin(object):
    def create_resource(self, db, node):
        filename = db.get_temp_filename(self.name, node)
        resource = resource = pypeliner.resources.TempFileResource(db.file_storage, self.name, self.node, filename,
            direct_write=self.direct_write,
            extensions=self.kwargs.get('extensions'))
        return resource


class TempInputFileArg(Arg, TempFileArgMixin):
    """ Temp input file argument

    Resolves to a filename for a temporary file.

    """
    def get_inputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.filename
    def allocate(self):
        self.resource.allocate()
    def pull(self):
        self.resource.pull()


class TempMergeFileArg(Arg, TempFileArgMixin):
    """ Temp input files merged along a single axis

    Resolves to a dictionary of filenames of temporary files.

    """
    def get_inputs(self):
        return self.resources
    def get_merge_inputs(self):
        return self.merge_inputs
    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved
    def allocate(self):
        for resource in self.resources:
            resource.allocate()
    def pull(self):
        for resource in self.resources:
            resource.pull()


class TempOutputFileArg(Arg, TempFileArgMixin):
    """ Temp output file argument

    Resolves to an output filename for a temporary file.  Finalizes with resource manager.

    """
    def get_outputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.write_filename
    def allocate(self):
        self.resource.allocate()
    def push(self):
        self.resource.push()


class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """
    def __init__(self, storage, name, node, axes, filename_creator, **kwargs):
        self.storage = storage
        self.name = name
        self.node = node
        self.axes = axes
        self.filename_creator = filename_creator
        self.resources = dict()
        self.kwargs = kwargs
    def __call__(self, *chunks):
        return self.__getitem__(*chunks)
    def __getitem__(self, *chunks):
        if len(self.axes) != len(chunks):
            raise ValueError('expected ' + str(len(self.axes)) + ' values for axes ' + str(self.axes))
        node = self.node
        for axis, chunk in zip(self.axes, chunks):
            node += pypeliner.identifiers.AxisInstance(axis, chunk)
        filename = self.filename_creator(self.name, node)
        resource = self.create_resource(self.storage, self.name, node, filename,
            direct_write=self.direct_write,
            extensions=self.kwargs.get('extensions'))
        if len(chunks) == 1:
            self.resources[chunks[0]] = resource
        else:
            self.resources[chunks] = resource
        resource.allocate()
        return resource.write_filename
    def __repr__(self):
        return '{0}.{1}({2},{3},{4},{5})'.format(
            FilenameCallback.__module__,
            FilenameCallback.__name__,
            self.name,
            self.node,
            self.axes,
            self.filename_creator)


class TempFilenameCallback(FilenameCallback):
    create_resource = pypeliner.resources.TempFileResource


class UserFilenameCallback(FilenameCallback):
    create_resource = pypeliner.resources.UserResource


class TempSplitFileArg(Arg, TempFileArgMixin):
    """ Temp output file arguments from a split

    Resolves to a filename callback that can be used to create a temporary filename for each chunk of the split on the
    given axis.  Finalizes with resource manager to move from temporary filename to final filename.

    """
    def setup_resources(self, db):
        super(TempSplitFileArg, self).setup_resources(db)
        if self.matches_origin:
            self.filename_callback = TempFilenameCallback(
                db.file_storage, self.name, self.node, self.axes,
                db.get_temp_filename_creator(), **self.kwargs)
        else:
            self.filenames = dict()
            for resource in self.resources:
                self.filenames[self.get_node_chunks(resource.node)] = resource.filename
            print self.filenames
            raise
    def get_outputs(self):
        return self.resources
    def resolve(self):
        if self.matches_origin:
            return self.filename_callback
        else:
            return self.filenames
    def get_split_chunks(self):
        if self.matches_origin:
            return self.filename_callback.resources.keys()
        else:
            return None
    def updatedb(self, db):
        if self.matches_origin:
            db.nodemgr.store_chunks(self.axes, self.node, self.filename_callback.resources.keys(), subset=self.axes_origin)
    def push(self):
        if self.matches_origin:
            for resource in self.filename_callback.resources.itervalues():
                resource.push()
        else:
            for resource in self.resources:
                resource.push()


class InputInstanceArg(Arg):
    """ Instance of a job as an argument

    Resolves to the instance of the given job for a specific axis.

    """
    def setup_resources(self, db):
        axis = self.kwargs['axis']
        self.chunk = dict(self.node)[axis]
    def resolve(self):
        return self.chunk


class InputChunksArg(Arg):
    """ Instance list of an axis as an argument

    Resolves to the list of chunks for the given axes.

    """
    def setup_resources(self, db):
        axis = self.kwargs['axis']
        self.chunks = list(db.nodemgr.retrieve_chunks(axis, self.node))
        if len(axis) == 1:
            self.chunks = [chunk[0] for chunk in self.chunks]
    def resolve(self):
        return self.chunks


class OutputChunksArg(Arg):
    """ Instance list of a job as an argument

    Sets the list of chunks for the given axes.

    """
    def resolve(self):
        return self
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value, subset=self.axes_origin)
