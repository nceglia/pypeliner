import unittest
import shutil
import os
import logging
import time

import pypeliner
import pypeliner.runskip
import pypeliner.workflow
import pypeliner.managed as mgd

from pypeliner.tests.tasks import *


script_directory = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(script_directory, 'pipeline')

exec_queue = pypeliner.execqueue.factory.create('local', [pypeliner.tests.tasks])
runskip = pypeliner.runskip.BasicRunSkip()


class scheduler_test(unittest.TestCase):

    input_filename = os.path.join(script_directory, 'scheduler_test.input')
    output_filename = os.path.join(script_directory, 'scheduler_test.output')

    input_n_filename = os.path.join(script_directory, 'scheduler_test.{byfile}.input')
    output_n_filename = os.path.join(script_directory, 'scheduler_test.{byfile}.output')
    output_n_template = os.path.join(script_directory, 'scheduler_test.{byfile}.template')

    input_1_filename = input_n_filename.format(byfile=1)
    input_2_filename = input_n_filename.format(byfile=2)

    output_1_filename = output_n_filename.format(byfile=1)
    output_2_filename = output_n_filename.format(byfile=2)

    log_filename = os.path.join(script_directory, './scheduler_test.log')

    try:
        os.remove(log_filename)
    except OSError:
        pass

    logging.basicConfig(level=logging.DEBUG, filename=log_filename, filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(pypeliner.helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - '))
    logging.getLogger('').addHandler(console)

    ctx = dict({'mem':1})

    def setUp(self):

        try:
            shutil.rmtree(pipeline_dir)
        except:
            pass

        try:
            shutil.rmtree(exc_prefix)
        except:
            pass

        try:
            os.remove(self.output_filename)
        except OSError:
            pass

        for chunk in (1, 2):
            try:
                os.remove(self.output_n_filename.format(**{'byfile':chunk}))
                os.remove(self.output_n_template.format(**{'byfile':chunk}))
            except OSError:
                pass

    def create_scheduler(self):

        scheduler = pypeliner.scheduler.Scheduler()
        scheduler.workflow_dir = pipeline_dir
        scheduler.max_jobs = 10
        return scheduler

    def test_simple_chunks1(self):

        workflow = pypeliner.workflow.Workflow()

        # Write a set of output files indexed by axis `byfile`
        workflow.transform(
            name='write_files',
            func=write_files,
            args=(mgd.OutputFile(self.output_n_filename, 'byfile'),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        for chunk in ('1', '2'):
            with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                output = output_file.readlines()
                self.assertEqual(output, ['file{0}\n'.format(chunk)])

    def test_simple_chunks2(self):

        workflow = pypeliner.workflow.Workflow()

        # Directly set the chunks indexed by axis `byfile`
        workflow.transform(
            name='set_chunks',
            func=set_chunks,
            ret=mgd.OutputChunks('byfile'))

        workflow.setobj(
            obj=mgd.OutputChunks('byfile', 'axis2'),
            value=['a', 'b'],
            axes=('byfile',))

        # Transform the input files indexed by axis `byfile` to output files
        # also indexed by axis `byfile`
        workflow.transform(
            name='do',
            axes=('byfile',),
            func=file_transform,
            args=(
                mgd.InputFile(self.input_n_filename, 'byfile'),
                mgd.OutputFile(self.output_n_filename, 'byfile'),
                mgd.InputInstance('byfile'),
                mgd.Template(self.output_n_template, 'byfile'),
                mgd.Template('{byfile}_{axis2}', 'byfile', 'axis2')))
        
        # Merge output files indexed by axis `byfile` into a single output file
        workflow.transform(
            name='merge',
            func=merge_file_byline,
            args=(
                mgd.InputFile(self.output_n_filename, 'byfile'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        merged_expected = []
        for chunk in ('1', '2'):
            self.assertTrue(os.path.exists(self.output_n_template.format(**{'byfile':chunk})))
            with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                output = output_file.readlines()
                expected = ['{0}\t{1}_{0}\n'.format(x, chunk) for x in ('a', 'b')]
                expected += [chunk + 'line' + str(line_num) + '\n' for line_num in range(1, 9)]
                self.assertEqual(output, expected)
                merged_expected += expected

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()
            self.assertEqual(output, merged_expected)

    def test_simple(self):

        workflow = pypeliner.workflow.Workflow()

        # Read data into a managed object
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform(
            name='do',
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('input_data').prop('some_string'),))

        # Write the object to an output file
        workflow.transform(
            name='write',
            func=write_stuff,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

    def test_dict_args(self):

        workflow = pypeliner.workflow.Workflow()

        workflow.transform(
            name='dict_arg_stuff',
            func=dict_arg_stuff,
            args=(
                {'1':mgd.OutputFile(self.output_1_filename),
                 '2':mgd.OutputFile(self.output_2_filename)},
                {'1':mgd.InputFile(self.input_1_filename),
                 '2':mgd.InputFile(self.input_2_filename)}))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        for file_num in (1, 2):
            with open(self.output_n_filename.format(byfile=file_num), 'r') as output_file:
                output = output_file.read()
            expected = 'line1{0}\nline2{0}\nline3{0}\nline4{0}\nline5{0}\nline6{0}\nline7{0}\nline8{0}\n'.format(file_num)
            self.assertEqual(output, expected)

    def test_simple_sub_workflow(self):

        workflow = pypeliner.workflow.Workflow(default_ctx=self.ctx)

        workflow.setobj(obj=mgd.OutputChunks('byfile'), value=(1, 2))

        workflow.transform(
            name='append_to_lines',
            axes=('byfile',),
            func=append_to_lines,
            args=(
                mgd.InputFile(self.input_n_filename, 'byfile'),
                '#',
                mgd.TempOutputFile('intermediate1', 'byfile')))

        workflow.subworkflow(
            name='sub_workflow_1',
            axes=('byfile',),
            func=create_workflow_1,
            args=(
                mgd.TempInputFile('intermediate1', 'byfile'),
                mgd.TempOutputFile('intermediate2', 'byfile')))

        workflow.transform(
            name='merge_files',
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('intermediate2', 'byfile'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        expected = ['0b0aline1#\n', '1b1aline2#\n', '2b2aline3#\n', '3b3aline4#\n',
            '4b4aline5#\n', '5b5aline6#\n', '6b6aline7#\n', '7b7aline8#-\n',
            '0b0aline1#\n', '1b1aline2#\n', '2b2aline3#\n', '3b3aline4#\n',
            '4b4aline5#\n', '5b5aline6#\n', '6b6aline7#\n', '7b7aline8#-\n']

        self.assertEqual(output, expected)

    def test_specify_input_filename(self):

        workflow = pypeliner.workflow.Workflow()

        # For single axis, only single value is supported
        input_filenames = {
            1:self.input_n_filename.format(byfile=1),
            2:self.input_n_filename.format(byfile=2),
        }

        # Merge a set of input files indexed by axis `byfile`
        workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))
        workflow.transform(
            name='merge_files',
            func=merge_file_byline,
            args=(
                mgd.InputFile('input_files', 'byfile', fnames=input_filenames),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

    def test_specify_input_filename_template(self):

        workflow = pypeliner.workflow.Workflow()

        # Merge a set of input files indexed by axis `byfile`
        workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))
        workflow.transform(
            name='merge_files',
            func=merge_file_byline,
            args=(
                mgd.InputFile('input_files', 'byfile', template=self.input_n_filename),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

    def test_specify_output_filename(self):

        workflow = pypeliner.workflow.Workflow()

        # For single axis, only single value is supported
        output_filenames = {
            1:self.output_n_filename.format(byfile=1),
            2:self.output_n_filename.format(byfile=2),
        }

        # Write a set of output files indexed by axis `byfile`
        workflow.transform(
            name='write_files',
            func=write_files,
            args=(mgd.OutputFile('output_files', 'byfile', fnames=output_filenames),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        for chunk in ('1', '2'):
            with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                output = output_file.readlines()
                self.assertEqual(output, ['file{0}\n'.format(chunk)])

    def test_specify_output_filename_template(self):

        workflow = pypeliner.workflow.Workflow()

        # Write a set of output files indexed by axis `byfile`
        workflow.transform(
            name='write_files',
            func=write_files,
            args=(mgd.OutputFile('output_files', 'byfile', template=self.output_n_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        for chunk in ('1', '2'):
            with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                output = output_file.readlines()
                self.assertEqual(output, ['file{0}\n'.format(chunk)])

    def test_set_multiple_axes(self):

        workflow = pypeliner.workflow.Workflow()

        # Merge a set of input files indexed by axis `byfile`
        workflow.setobj(mgd.OutputChunks('byfile', 'bychar'), ((1, 'a'), (1, 'b'), (2, 'a')))
        workflow.transform(
            name='merge_files',
            func=merge_file_byline,
            args=(
                mgd.InputFile('input_files', 'byfile', template=self.input_n_filename),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

    def test_tempfile(self):

        workflow = pypeliner.workflow.Workflow()

        # Write the name of the temp file produced by pypeliner
        # into an output file
        workflow.transform(
            name='write_files',
            func=check_temp,
            args=(
                mgd.OutputFile(self.output_filename),
                mgd.TempSpace('temp_space')))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, [os.path.join(pipeline_dir, 'tmp/temp_space')])

    def test_missing_temporary(self):

        workflow = pypeliner.workflow.Workflow()

        workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))

        workflow.transform(
            name='read',
            axes=('byfile',),
            func=do_file_stuff,
            args=(
                mgd.InputFile('input_files', 'byfile', template=self.input_n_filename),
                mgd.TempOutputFile('temp_files', 'byfile'),
                'extras'))

        workflow.transform(
            name='merge_files',
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('temp_files', 'byfile'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        time.sleep(1)
        os.utime(self.input_1_filename, None)
        os.remove(self.output_filename)

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        expected_output = []
        for file_id in xrange(1, 2+1):
            for line in xrange(1, 8+1):
                expected_output.append('{}{}line{}\n'.format(line-1, 'extras', line))

        self.assertEqual(output, expected_output)

    def failing_missing_temporary2(self):

        workflow = pypeliner.workflow.Workflow()

        workflow.transform(
            name='do_file_stuff_1',
            func=do_file_stuff,
            args=(
                mgd.InputFile(self.input_filename),
                mgd.TempOutputFile('temp_file1'),
                '1'))

        workflow.transform(
            name='do_file_stuff_2',
            func=do_file_stuff,
            args=(
                mgd.TempInputFile('temp_file1'),
                mgd.TempOutputFile('temp_file2'),
                '2'))

        workflow.transform(
            name='do_file_stuff_3',
            func=do_file_stuff,
            args=(
                mgd.TempInputFile('temp_file2'),
                mgd.OutputFile(self.output_filename),
                '3'))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        time.sleep(1)
        os.remove(self.output_filename)

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        expected_output = []
        for file_id in xrange(1, 2+1):
            for line in xrange(1, 8+1):
                expected_output.append('{}{}line{}\n'.format(line-1, 'extras', line))

        self.assertEqual(output, expected_output)

    def test_simple_create_all(self):

        workflow = pypeliner.workflow.Workflow()

        # Read data into a managed object
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform(
            name='do',
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('input_data').prop('some_string'),))

        # Write the object to an output file
        workflow.transform(
            name='write',
            func=write_stuff,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.TempOutputFile('output_file')))

        scheduler = self.create_scheduler()
        scheduler.prune = False
        scheduler.cleanup = False
        scheduler.run(workflow, exec_queue, runskip)

        with open(os.path.join(pipeline_dir, 'tmp/output_file'), 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

        self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/input_data._o')))
        self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/output_data._o')))

    def test_cycle(self):

        workflow = pypeliner.workflow.Workflow()

        # Read data into a managed object, but also add a superfluous
        # input argument called `cyclic`, which is generated by a
        # downstream job
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(
                mgd.InputFile(self.input_filename),
                mgd.TempInputObj('cyclic')))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform(
            name='do',
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('input_data').prop('some_string'),))

        # Write the object to an output file, and also include `cyclic`
        # as an output so as to create a cycle in the dependency graph
        workflow.transform(
            name='write',
            func=write_stuff,
            ret=mgd.TempOutputObj('cyclic'),
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()

        self.assertRaises(pypeliner.graph.DependencyCycleException, scheduler.run, workflow, exec_queue, runskip)

    def test_commandline_simple(self):

        workflow = pypeliner.workflow.Workflow(default_ctx=self.ctx)

        # Copy input to output file using linux `cat`
        workflow.commandline(
            name='do',
            args=(
                'cat',
                mgd.InputFile(self.input_filename),
                '|',
                'cat',
                '>', mgd.OutputFile(self.output_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

    def test_single_object_split(self):

        workflow = pypeliner.workflow.Workflow(default_ctx=self.ctx)

        # Read data into a managed object, which is a string
        workflow.transform(
            name='read', 
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))

        # Split the string into individual characters
        workflow.transform(
            name='splitbychar',
            func=split_stuff,
            ret=mgd.TempOutputObj('input_data', 'bychar'),
            args=(mgd.TempInputObj('input_data'),))
        
        # Modify each single character string, appending `-`
        workflow.transform(
            name='do',
            axes=('bychar',),
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data', 'bychar'),
            args=(mgd.TempInputObj('input_data', 'bychar').prop('some_string'),))
        
        # Merge the modified strings
        workflow.transform(
            name='mergebychar', 
            func=merge_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('output_data', 'bychar'),))
        
        # Write the modified merged string to an output file
        workflow.transform(
            name='write', 
            func=write_stuff,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['l-i-n-e-1-\n', '-l-i-n-e-2-\n', '-l-i-n-e-3-\n', '-l-i-n-e-4-\n', '-l-i-n-e-5-\n', '-l-i-n-e-6-\n', '-l-i-n-e-7-\n', '-l-i-n-e-8-'])

    def test_split_getinstance(self):

        workflow = pypeliner.workflow.Workflow()

        # Split input file by line and output one file per line
        workflow.transform(
            name='splitbyline',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                1,
                mgd.TempOutputFile('input_filename', 'byline')))

        # Append the `instance` of the split (basically the index of the line)
        # to each temporary file `input_filename` and output to a new file
        workflow.transform(
            name='append',
            axes=('byline',),
            func=append_to_lines_instance,
            args=(
                mgd.TempInputFile('input_filename', 'byline'),
                mgd.InputInstance('byline'),
                mgd.TempOutputFile('output_filename', 'byline')))
        
        # Merge files and output
        workflow.transform(
            name='mergebyline',
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('output_filename', 'byline'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line10\n', 'line21\n', 'line32\n', 'line43\n', 'line54\n', 'line65\n', 'line76\n', 'line87\n'])

    def test_split_getinstances(self):

        workflow = pypeliner.workflow.Workflow()

        # Split file by line and output a single line per temporary output
        # file named `input_filename`
        workflow.transform(
            name='splitbyline',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                1,
                mgd.TempOutputFile('input_filename', 'byline')))

        # Write the list of chunks (line indexes) on the axis `byline` produced
        # by splitting the input file
        workflow.transform(
            name='writelist',
            func=write_list,
            args=(
                mgd.InputChunks('byline'),
                mgd.OutputFile(self.output_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['01234567'])

    def test_multiple_object_split(self):

        workflow = pypeliner.workflow.Workflow()

        # Read input file and store in managed input object, which is 
        # a string of the file contents
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))

        # Split the string by line and store as a new object
        workflow.transform(
            name='splitbyline',
            func=split_by_line,
            ret=mgd.TempOutputObj('input_data', 'byline'),
            args=(mgd.TempInputObj('input_data'),))
        
        # Split each of the resulting strings by character and 
        # output as single character strings
        workflow.transform(
            name='splitbychar',
            axes=('byline',),
            func=split_by_char,
            ret=mgd.TempOutputObj('input_data', 'byline', 'bychar'),
            args=(mgd.TempInputObj('input_data', 'byline'),))

        # Transform each single character string, appending a `-` character
        workflow.transform(
            name='do',
            axes=('byline', 'bychar'),
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data', 'byline', 'bychar'),
            args=(mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'),))

        # Merge modified strings along the `bychar` axis
        workflow.transform(
            name='mergebychar',
            axes=('byline',),
            func=merge_stuff,
            ret=mgd.TempOutputObj('output_data', 'byline'),
            args=(mgd.TempInputObj('output_data', 'byline', 'bychar'),))

        # Merge modified strings along the `byline` axis
        workflow.transform(
            name='mergebyline',
            func=merge_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('output_data', 'byline'),))

        # Write the merged string to an output file
        workflow.transform(
            name='write',
            func=write_stuff,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['l-i-n-e-1-l-i-n-e-2-l-i-n-e-3-l-i-n-e-4-l-i-n-e-5-l-i-n-e-6-l-i-n-e-7-l-i-n-e-8-'])

        workflow = pypeliner.workflow.Workflow()

        # Redo the same steps and ensure that each step is skipped because each
        # step is up to date
        workflow.transform(
            name='read',
            func=do_assert,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))
        workflow.transform(
            name='splitbyline',
            func=do_assert,
            ret=mgd.TempOutputObj('input_data', 'byline'),
            args=(mgd.TempInputObj('input_data'),))
        workflow.transform(
            name='splitbychar',
            axes=('byline',),
            func=do_assert,
            ret=mgd.TempOutputObj('input_data', 'byline', 'bychar'),
            args=(mgd.TempInputObj('input_data', 'byline'),))
        workflow.transform(
            name='do',
            axes=('byline', 'bychar'),
            func=do_assert,
            ret=mgd.TempOutputObj('output_data', 'byline', 'bychar'),
            args=(mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'),))
        workflow.transform(
            name='mergebychar',
            axes=('byline',),
            func=do_assert,
            ret=mgd.TempOutputObj('output_data', 'byline'),
            args=(mgd.TempInputObj('output_data', 'byline', 'bychar'),))
        workflow.transform(
            name='mergebyline',
            func=do_assert,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('output_data', 'byline'),))
        workflow.transform(
            name='write',
            func=do_assert,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

    def test_multiple_file_split(self):

        workflow = pypeliner.workflow.Workflow(default_ctx=self.ctx)

        # Split input file into 4 lines per output file (axis `byline_a`)
        workflow.transform(
            name='split_byline_a',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                4,
                mgd.TempOutputFile('input_data', 'byline_a')))

        # Split again, this time with 2 lines per output file (axis `byline_b`)
        workflow.transform(
            name='split_byline_b',
            axes=('byline_a',),
            func=split_file_byline,
            args=(
                mgd.TempInputFile('input_data', 'byline_a'),
                2,
                mgd.TempOutputFile('input_data', 'byline_a', 'byline_b')))

        # Modify each file independently, adding the instance of this job on the
        # `byline_a` axis
        workflow.transform(
            name='do',
            axes=('byline_a', 'byline_b'),
            func=do_file_stuff,
            args=(
                mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'),
                mgd.InputInstance('byline_a')))

        # Merge along the `byline_b` axis
        workflow.transform(
            name='merge_byline_b',
            axes=('byline_a',),
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                mgd.TempOutputFile('output_data', 'byline_a')))

        # Merge along the `byline_a` axis
        workflow.transform(
            name='merge_byline_a',
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('output_data', 'byline_a'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.cleanup = False
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['00line1\n', '10line2\n', '00line3\n', '10line4\n', '01line5\n', '11line6\n', '01line7\n', '11line8\n'])

        tmp_input_data_filenames = [os.path.join(pipeline_dir, 'tmp/byline_a/0/input_data'), os.path.join(pipeline_dir, 'tmp/byline_a/1/input_data')]
        tmp_data_checks = [['line1\n', 'line2\n', 'line3\n', 'line4\n'], ['line5\n', 'line6\n', 'line7\n', 'line8\n']]
        for tmp_input_data_filename, tmp_data_check in zip(tmp_input_data_filenames, tmp_data_checks):
            with open(tmp_input_data_filename, 'r') as tmp_input_data_file:
                tmp_input_data = tmp_input_data_file.readlines()
                self.assertEqual(tmp_input_data, tmp_data_check)

    def test_rerun_simple(self):

        workflow = pypeliner.workflow.Workflow()

        self.assertFalse(os.path.exists(self.output_filename))

        # Modify input file, append `!` to each line
        workflow.transform(
            name='step1',
            func=append_to_lines,
            args=(
                mgd.InputFile(self.input_filename),
                '!',
                mgd.TempOutputFile('appended')))

        # Copy the file
        workflow.transform(
            name='step2',
            func=copy_file,
            args=(
                mgd.TempInputFile('appended'),
                mgd.TempOutputFile('appended_copy')))

        # This job should copy the file again but does nothing, raising
        # an exception when the pipeline is run
        workflow.transform(
            name='step3',
            func=do_nothing,
            args=(
                mgd.TempInputFile('appended_copy'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.cleanup = False
        self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue, runskip)

        workflow = pypeliner.workflow.Workflow()

        # Redo the previous steps, ensuring the first two steps are not
        # run since their dependencies are up to date, and make sure the final
        # copy is done correctly
        workflow.transform(
            name='step1',
            func=do_assert,
            args=(
                mgd.InputFile(self.input_filename),
                '!',
                mgd.TempOutputFile('appended')))
        workflow.transform(
            name='step2',
            func=do_assert,
            args=(
                mgd.TempInputFile('appended'),
                mgd.TempOutputFile('appended_copy')))
        workflow.transform(
            name='step3',
            func=copy_file,
            args=(
                mgd.TempInputFile('appended_copy'),
                mgd.OutputFile(self.output_filename)))

        scheduler.cleanup = True
        scheduler.run(workflow, exec_queue, runskip)

        # The temporary files should have been cleaned up
        self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
        self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])

    def test_repopulate(self):

        workflow = pypeliner.workflow.Workflow()

        self.assertFalse(os.path.exists(self.output_filename))

        # Modify input file, append `!` to each line
        workflow.transform(
            name='step1',
            func=append_to_lines,
            args=(
                mgd.InputFile(self.input_filename),
                '!',
                mgd.TempOutputFile('appended')))

        # Copy the file
        workflow.transform(
            name='step2',
            func=copy_file,
            args=(
                mgd.TempInputFile('appended'),
                mgd.TempOutputFile('appended_copy')))

        # This job should copy the file again but does nothing, raising
        # an exception when the pipeline is run
        workflow.transform(
            name='step3',
            func=do_nothing,
            args=(
                mgd.TempInputFile('appended_copy'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue, runskip)

        self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))

        workflow = pypeliner.workflow.Workflow()

        # Rerun the same pipeline in repopulate mode, this time the final
        # copy is done correctly
        workflow.transform(
            name='step1',
            func=append_to_lines,
            args=(
                mgd.InputFile(self.input_filename),
                '!',
                mgd.TempOutputFile('appended')))
        workflow.transform(
            name='step2',
            func=copy_file,
            args=(
                mgd.TempInputFile('appended'),
                mgd.TempOutputFile('appended_copy')))
        workflow.transform(
            name='step3',
            func=copy_file,
            args=(
                mgd.TempInputFile('appended_copy'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.cleanup = False
        repopulate_runskip = pypeliner.runskip.BasicRunSkip(repopulate=True)
        scheduler.run(workflow, exec_queue, repopulate_runskip)

        # The temporary files should have been cleaned up
        self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
        self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])

    def test_rerun_multiple_file_split(self):

        workflow = pypeliner.workflow.Workflow()

        # Split input file into 4 lines per output file (axis `byline_a`)
        workflow.transform(
            name='split_byline_a',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                4,
                mgd.TempOutputFile('input_data', 'byline_a')))

        # Split again, this time with 2 lines per output file (axis `byline_b`)
        workflow.transform(
            name='split_byline_b',
            axes=('byline_a',),
            func=split_file_byline,
            args=(
                mgd.TempInputFile('input_data', 'byline_a'),
                2,
                mgd.TempOutputFile('input_data', 'byline_a', 'byline_b')))

        # Modify each file independently, adding the instance of this job on the
        # `byline_a` axis, fail here
        workflow.transform(
            name='do',
            axes=('byline_a', 'byline_b'),
            func=do_nothing,
            args=(
                mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                '!',
                mgd.TempOutputFile('output_data', 'byline_a', 'byline_b')))

        # Merge along the `byline_b` axis
        workflow.transform(
            name='merge_byline_b',
            axes=('byline_a',),
            func=do_nothing,
            args=(
                mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                mgd.TempOutputFile('output_data', 'byline_a')))

        # Merge along the `byline_a` axis
        workflow.transform(
            name='merge_byline_a',
            func=do_nothing,
            args=(
                mgd.TempInputFile('output_data', 'byline_a'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue, runskip)

        workflow = pypeliner.workflow.Workflow()

        # Split input file into 4 lines per output file (axis `byline_a`)
        workflow.transform(
            name='split_byline_a',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                4,
                mgd.TempOutputFile('input_data', 'byline_a')))

        # Split again, this time with 2 lines per output file (axis `byline_b`)
        workflow.transform(
            name='split_byline_b',
            axes=('byline_a',),
            func=split_file_byline,
            args=(
                mgd.TempInputFile('input_data', 'byline_a'),
                2,
                mgd.TempOutputFile('input_data', 'byline_a', 'byline_b')))

        # Modify each file independently, adding the instance of this job on the
        # `byline_a` axis
        workflow.transform(
            name='do',
            axes=('byline_a', 'byline_b'),
            func=append_to_lines,
            args=(
                mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                '!',
                mgd.TempOutputFile('output_data', 'byline_a', 'byline_b')))

        # Merge along the `byline_b` axis, fail here
        workflow.transform(
            name='merge_byline_b',
            axes=('byline_a',),
            func=do_nothing,
            args=(
                mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                mgd.TempOutputFile('output_data', 'byline_a')))

        # Merge along the `byline_a` axis
        workflow.transform(
            name='merge_byline_a',
            func=do_nothing,
            args=(
                mgd.TempInputFile('output_data', 'byline_a'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue, runskip)

        workflow = pypeliner.workflow.Workflow()

        # Split input file into 4 lines per output file (axis `byline_a`)
        workflow.transform(
            name='split_byline_a',
            func=split_file_byline,
            args=(
                mgd.InputFile(self.input_filename),
                4,
                mgd.TempOutputFile('input_data', 'byline_a')))

        # Split again, this time with 2 lines per output file (axis `byline_b`)
        workflow.transform(
            name='split_byline_b',
            axes=('byline_a',),
            func=split_file_byline,
            args=(
                mgd.TempInputFile('input_data', 'byline_a'),
                2,
                mgd.TempOutputFile('input_data', 'byline_a', 'byline_b')))

        # Modify each file independently, adding the instance of this job on the
        # `byline_a` axis
        workflow.transform(
            name='do',
            axes=('byline_a', 'byline_b'),
            func=append_to_lines,
            args=(
                mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                '!',
                mgd.TempOutputFile('output_data', 'byline_a', 'byline_b')))

        # Merge along the `byline_b` axis
        workflow.transform(
            name='merge_byline_b',
            axes=('byline_a',),
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                mgd.TempOutputFile('output_data', 'byline_a')))

        # Merge along the `byline_a` axis
        workflow.transform(
            name='merge_byline_a',
            func=merge_file_byline,
            args=(
                mgd.TempInputFile('output_data', 'byline_a'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])

    def test_object_identical(self):

        workflow = pypeliner.workflow.Workflow()

        # Read data into a managed object
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename),))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform(
            name='do',
            func=do_stuff,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('input_data').prop('some_string'),))

        # Write the object to an output file
        workflow.transform(
            name='write',
            func=write_stuff,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename)))

        scheduler = self.create_scheduler()
        scheduler.cleanup = False
        scheduler.run(workflow, exec_queue, runskip)

        with open(self.output_filename, 'r') as output_file:
            output = output_file.readlines()

        self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

        print 'restarting'
        time.sleep(1)

        workflow = pypeliner.workflow.Workflow()

        shutil.copyfile(self.input_filename, self.input_filename+'.tmp')

        # Read the same data into a managed object
        workflow.transform(
            name='read',
            func=read_stuff,
            ret=mgd.TempOutputObj('input_data'),
            args=(mgd.InputFile(self.input_filename+'.tmp'),))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform(
            name='do',
            func=do_assert,
            ret=mgd.TempOutputObj('output_data'),
            args=(mgd.TempInputObj('input_data').prop('some_string'),))

        # Write the object to an output file
        workflow.transform(
            name='write',
            func=do_assert,
            args=(
                mgd.TempInputObj('output_data'),
                mgd.OutputFile(self.output_filename),))

        scheduler = self.create_scheduler()
        scheduler.run(workflow, exec_queue, runskip)


if __name__ == '__main__':
    unittest.main()

