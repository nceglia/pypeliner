"""
Interface classes used to describe objects managed by the pipeline system.

Objects of each type can be used as placeholder arguments to :py:func:`pypeliner.scheduler.Scheduler.transform` or
:py:func:`pypeliner.scheduler.Scheduler.commandline`.  During pipeline execution, the placeholder argument will be
replaced by the appropriate file or object to create the function or command line arguments.

Managed classes have a common set of parameters:

* name - An identifier for the managed object
* axes - The axes on which the managed object is defined

Axes relate to parallelism.  A managed object with an empty list for axes has a single instance in the system.  A
managed object with a single axis will have as many instances as there are chunks defined for that axis.  Axes can also
be nested to arbitrary depth.

For example, suppose we are running the same analysis on 2 datasets, thus our first axis is 'dataset' with 2 chunks 'A'
and 'B'.  Each dataset is split by line, thus our second axis is 'line'.  Each dataset may have a different number of
lines, and as such the number of chunks for the 'line' axis may be different between dataset 'A' and 'B'.  Thus if
dataset 'A' has 2 lines nad 'B' has 1 line, a managed object defined on the axes 'dataset', 'line' will have the
following instances: ``{'dataset':'A', 'line':1}``, ``{'dataset':'A', 'line':2}``, ``{'dataset':'B', 'line':1}``.

A managed object will resolve to a function or command line argument dependent on whether it is a regular input/output,
merge input or split output.  Regular inputs/outputs have the same axes as the job to which they are given.  Merge
inputs are inputs with a single additional axis, the merge axis.  Split outputs are outputs with a single additional
axis, the split axis.

"""

import collections

import arguments

class JobArgMismatchException(Exception):
    def __init__(self, name, axes, node):
        self.name = name
        self.axes = axes
        self.node = node
        self.job_name = 'unknown'
    def __str__(self):
        return 'arg {0} with axes {1} does not match job {2} with axes {3}'.format(self.name, self.axes, self.job_name, tuple(a[0] for a in self.node))

def create_arg(resmgr, nodemgr, mgd, node):
    """ Translate a managed user argument into an internally used Arg object """
    if isinstance(mgd, Managed):
        return mgd.create_arg(resmgr, nodemgr, node)
    else:
        return mgd

class Managed(object):
    """ Interface class used to represent a managed data """
    def __init__(self, name, *axes, **kwargs):
        if name is not None and type(name) != str:
            raise ValueError('name of argument must be string')
        if type(axes) != tuple:
            raise ValueError('axes must be a tuple')
        self.name = name
        self.axes = axes
        self.kwargs = kwargs
    def _create_arg(self, resmgr, nodemgr, node, normal=None, splitmerge=None, **kwargs):
        common = 0
        for node_axis, axis in zip((a[0] for a in node), self.axes):
            if node_axis != axis:
                break
            common += 1
        axes_specific = self.axes[common:]
        if len(axes_specific) == 0 and normal is not None:
            return normal(resmgr, nodemgr, self.name, node[:common], **kwargs)
        elif len(axes_specific) == 1 and splitmerge is not None:
            return splitmerge(resmgr, nodemgr, self.name, node[:common], axes_specific[0], **kwargs)
        else:
            raise JobArgMismatchException(self.name, self.axes, node)

class Template(Managed):
    """ Represents a name templated by axes 

    `Template` objects will resolve the specified `name` templated by the given
    `axes`.  `name` should be a format string, with named fields that match 
    the names of the axes.  

    For instance, `Template('{case}_details', 'case')` will resolve to the
    strings 'tumour_details' and 'normal_details' if the `case` axis has chunks
    'tumour' and 'normal'.

    :param name: The format string to be resolved by pypeliner.  Each axis
                 should appear at least once as a named field in the format
                 string.
    :param axes: The axes to use to resolve `name`.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TemplateArg, splitmerge=arguments.MergeTemplateArg)

class TempFile(Managed):
    """ Represents a temp file the can be written to but is not a dependency

    `TempFile` objects will resolve to the path of a temporary file located
    in pypeliner's temporary file space.  If axes are given, a new temporary
    file will be created for each chunk of the given axes.  The file is not
    guaranteed to exist after the job referencing the `TempFile` has finished
    execution.

    :param name: The name of the temporary file.  The temp file will have
                 this filename, but different instances for different axis
                 chunks will be located in different directories.
    :param axes: The axes for the file.  This should be identical to the
                 axes of the referencing job.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempFileArg)

class InputFile(Managed):
    """ Interface class used to represent a user specified managed file input

    `InputFile` objects will resolve the specified `name` templated by the given
    `axes`.  `name` should be a format string, with named fields that match 
    the names of the axes.  The modification time of the file will be used to
    determine if the file has been modified more recently than a job's outputs,
    in order to determine if a job must be run.

    For instance, `InputFile('{case}.bam', 'case')` will resolve to the
    strings 'tumour.bam' and 'normal.bam' if the `case` axis has chunks
    'tumour' and 'normal'.

    :param name: The name of the input file.  Each axis should appear at least
                 once as a named field in the filename.
    :param axes: The axes for the input file.

    For a merge input, `InputFile` will resolve to a dictionary of filenames
    as specified above, with chunks of the merge axis as keys.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.InputFileArg, splitmerge=arguments.MergeFileArg, **self.kwargs)

class OutputFile(Managed):
    """ Interface class used to represent a user specified managed file output

    `OutputFile` objects will resolve the specified filename templated by the
    given  axes.  `name` should be a format string, with named fields that match 
    the names of the axes.  An `OutputFile` of the given name and axes is 
    associated with a single job that creates that file.

    For instance, `OutputFile('{case}.bam', 'case')` will resolve to the
    strings 'tumour.bam' and 'normal.bam' if the `case` axis has chunks
    'tumour' and 'normal'.

    :param name: The name of the output file.  Each axis should appear at least
                 once as a named field in the filename.
    :param axes: The axes for the output file.

    For a split output, `OutputFile` will resolve to a callback function taking
    the chunk of the split axis as its only parameter and returning the filename
    for that chunk.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.OutputFileArg, splitmerge=arguments.SplitFileArg, **self.kwargs)

class TempInputObj(Managed):
    """ Interface class used to represent a managed object input

    `TempInputObj` objects will resolve to an object managed by the pipeline
    system.  The contents of the object are used for dependency tracking, as
    described for :py:class:`pypeliner.managed.TempOutputObj`.

    :param name: The name of the object.
    :param axes: The axes for the object.

    For a merge input, `TempInputObj` will resolve to a dictionary of objects
    with chunks of the merge axis as keys.

    """
    def prop(self, prop_name):
        """
        Resolve to a property of the object instead of the object itself.

        :param name: The name of the property.
        """
        return TempInputObjExtract(self.name, self.axes, lambda a: getattr(a, prop_name))
    def extract(self, func):
        """
        Resolve to the return value of the given function called on the object
        rather than the object itself.
        
        :param func: The function to be executed on the object.

        .. admonition:: Warning about state

            The function provided should not have any state as this state 
            cannot be tracked by the dependency system.  Appropriate uses
            are a lambda function that accesses a dictionary entry or 
            performs a fixed calculation.
        """
        return TempInputObjExtract(self.name, self.axes, func)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputObjArg, splitmerge=arguments.TempMergeObjArg)

class TempOutputObj(Managed):
    """ Interface class used to represent a managed object output

    `TempOutputObj` objects are only appropriate as return values for calls 
    to :py:func:`pypeliner.scheduler.Scheduler.transform`.  The object returned
    by the function executed for a transform job will be stored by the pipeline
    using `pickle`.

    If returning a user specified type, it is advisable to add a `__eq__` method.
    Dependency tracking for objects is done by checking if the object has changed
    since the last call that created the object, and will call `__eq__`, or will
    default to comparing `__dict__`.

    :param name: The name of the object.
    :param axes: The axes for the object.

    For a split output, the pipeline system expects a dictionary of objects
    with chunks of the split axis as keys.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempOutputObjArg, splitmerge=arguments.TempSplitObjArg)

class TempInputObjExtract(Managed):
    """ Interface class used to represent a property of a managed
    input object """
    def __init__(self, name, axes, func):
        Managed.__init__(self, name, *axes)
        self.func = func
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputObjArg, splitmerge=arguments.TempMergeObjArg, func=self.func)

class TempInputFile(Managed):
    """ Interface class used to represent a managed temporary file input

    `TempInputFile` objects will resolve to a filename in the temporary file
    space of the pipeline.  Temporary files are subject to garbage collection.

    :param name: The name of the temporary file, basename only (no path
                 information).
    :param axes: The axes for the file.

    For a merge input, `InputFile` will resolve to a dictionary of filenames,
    with chunks of the merge axis as keys.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputFileArg, splitmerge=arguments.TempMergeFileArg)

class TempOutputFile(Managed):
    """ Interface class used to represent a managed temporary file output

    `TempOutputFile` objects will resolve to a filename in the temporary file
    space of the pipeline.  Temporary files are subject to garbage collection.

    :param name: The name of the temporary file, basename only (no path
                 information).
    :param axes: The axes for the file.

    For a split output, `TempOutputFile` will resolve to a callback function taking
    the chunk of the split axis as its only parameter and returning the filename
    for that chunk.

    """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempOutputFileArg, splitmerge=arguments.TempSplitFileArg)

class InputInstance(Managed):
    """ Interface class used to represent an input chunk associated with the
    job instance

    :param axis: The axis of interest for which to obtain chunk information.

    Resolves to the chunk of its job's instance for a given axis.

    """
    def __init__(self, axis):
        self.axis = axis
    def create_arg(self, resmgr, nodemgr, node):
        return arguments.InputInstanceArg(resmgr, nodemgr, node, self.axis)

class InputChunks(Managed):
    """ Interface class used to represent an input chunk list for a specific axis

    :param axes: The axes of interest for which to obtain a list of chunks.

    `InputChunks` acts similar to a merge.  The specified axes should match
    the axes of its job, with a single additional axis as for a merge.  
    Resolves to a list of chunks for the given 'merge' axis.

    """
    def __init__(self, *axes):
        Managed.__init__(self, None, *axes)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=arguments.InputChunksArg)

class OutputChunks(Managed):
    """ Interface class used to represent an output that defines the list of
    chunks for a specific axis

    :param axes: The axes for which chunks will be set.

    `OutputChunks` acts similar to a split object.  `OutputChunks` objects are
    only appropriate as return values for calls to
    :py:func:`pypeliner.scheduler.Scheduler.transform`.  The specified axes should
    match the axes of its job, with a single additional axis as for a split.  The
    pipeline system expects the job function to return a list, which is then
    interpreted as the list of chunks for the given 'split' axis.

    """
    def __init__(self, *axes):
        Managed.__init__(self, None, *axes)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=arguments.OutputChunksArg)

