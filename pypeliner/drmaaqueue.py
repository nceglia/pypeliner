import drmaa
import logging
import os
import time

from execqueue import LocalJobQueue, ReceiveError, qsub_format_name

import delegator
import helpers

decode_status = {
    drmaa.JobState.UNDETERMINED: 'process status cannot be determined',
    drmaa.JobState.QUEUED_ACTIVE: 'job is queued and active',
    drmaa.JobState.SYSTEM_ON_HOLD: 'job is queued and in system hold',
    drmaa.JobState.USER_ON_HOLD: 'job is queued and in user hold',
    drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
    drmaa.JobState.RUNNING: 'job is running',
    drmaa.JobState.SYSTEM_SUSPENDED: 'job is system suspended',
    drmaa.JobState.USER_SUSPENDED: 'job is user suspended',
    drmaa.JobState.DONE: 'job finished normally',
    drmaa.JobState.FAILED: 'job finished, but failed'
}

class DrmaaJob(object):
    """ Encapsulate a running job created using drmaa
    """
    def __init__(self, ctx, name, sent, temps_dir, modules, native_spec, session):
        self.name = name
        self.native_spec = native_spec
        self.session = session
        self.temps_dir = temps_dir
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(self.temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(self.temps_dir, 'job.err')
        self.debug_filenames['resources'] = os.path.join(self.temps_dir, 'resources.txt')
        for filename in self.debug_filenames.itervalues():
            helpers.saferemove(filename)
        
        job_template = self.session.createJobTemplate()
        job_template.remoteCommand = self.command[0]
        job_template.args = self.command[1:]
        
        ctx, native_spec = self._parse_native_ctx(ctx)
        native_spec, export_local_env = self._parse_native_spec(native_spec)
        if export_local_env:
            job_template.jobEnvironment = os.environ
        
        job_template.nativeSpecification = self._create_native_spec(native_spec, ctx)
        job_template.outputPath = ':' + self.debug_filenames['job stdout']
        job_template.errorPath = ':' + self.debug_filenames['job stderr']
        job_template.jobName = qsub_format_name(self.name)
        
        self.job_id = self.session.runJob(job_template)
        self.session.deleteJobTemplate(job_template)

        self.job_info = None
    
    @property
    def finished(self):
        """ Get job finished boolean.
        """
        if self.job_info is not None:
            return True
        
        job_status = self.session.jobStatus(self.job_id)
            
        if job_status in [drmaa.JobState.QUEUED_ACTIVE, drmaa.JobState.RUNNING, drmaa.JobState.UNDETERMINED]:
            return False
        
        else:
            self.job_info = self.session.wait(self.job_id)
       
            return True
    
    def finalize(self):
        assert self.finished
        
        self._write_resource_usage()
        
        if int(self.job_info.exitStatus) != 0:
            raise ReceiveError(self._create_error_text('drmma error'))
        
        self.received = self.delegated.finalize()
                    
        if self.received is None:
            raise ReceiveError(self._create_error_text('receive error'))
    
    def _create_error_text(self, desc):
        """ Create error text string.

        Args:
            desc (str): error description

        Returns:
            str: multi-line error text
        """

        error_text = list()

        error_text.append('{0} for job: {1}'.format(desc, self.name))
                
        error_text.append('qsub id: {0}'.format(self.job_id))
        
        if self.job_info.wasAborted:
            raise error_text.append('job was aborted')
     
        elif self.job_info.hasSignal:
            error_text.append('job finished due to signal {0}.'.format(self.job_info.terminatedSignal))
        
            if self.job_info.hasCoreDump:
                error_text.append('a core dump is available for job')
        
        else:
            error_text.append('job finished with unclear conditions')
                
        cmd_str = ' '.join(self.command)
        
        error_text.append('delegator command: {0}'.format(cmd_str))

        error_text.append('memory consumed: {0}'.format(self.job_info.resourceUsage['maxvmem']))
            
        error_text.append('job exit status: {0}'.format(self.job_info.exitStatus))

        for debug_type, debug_filename in self.debug_filenames.iteritems():
            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']
            
                continue
            
            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']
                
                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)
    
    def _create_native_spec(self, native_spec, ctx):
        return '-w w ' + native_spec.format(**ctx)
    
    def _parse_native_ctx(self, ctx):
        if 'native_spec' in ctx:
            native_spec = ctx['native_spec']
            
            del ctx['native_spec']
        
        else:
            native_spec = self.native_spec
            
        return ctx, native_spec
    
    def _parse_native_spec(self, native_spec):
        if '-V' in native_spec:
            native_spec = native_spec.replace('-V', '')
        
            export_local_env = True
    
        else:
            export_local_env = False
        
        return native_spec, export_local_env
    
    def _write_resource_usage(self):
        resources_text = []
        
        resources_text.append('=' * 80)
        
        max_len = max([len(x) for x in self.job_info.resourceUsage.keys()])
        
        for key, value in sorted(self.job_info.resourceUsage.items()):
            num_spaces = max_len - len(key) + 1
            
            if key in ['acct_maxvmem', 'mem', 'maxvmem', 'vmem']:
                value = '{0}G'.format(round(float(value) / 1e9, 2))
            
            if key in ['exit_status', 'failed', 'signal', 'slots']:
                value = str(int(float(value)))
            
            if key in ['end_time', 'start_time', 'submission_time']:
                value = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(value)))
            
            out_str = key + ' ' * num_spaces + value
            
            resources_text.append(out_str)
            
        with open(self.debug_filenames['resources'], 'w') as fh:    
            fh.write('\n'.join(resources_text))

class DrmaaJobQueue:
    """ Maintain a list of running jobs executed synchronously using
    drmaa, with the ability to wait for jobs and return completed jobs
    """
    def __init__(self, modules, native_spec):
        self.modules = modules
        
        self.native_spec = native_spec
        
        self.jobs = dict()
        
        self.name_islocal = dict()
        
        self.local_queue = LocalJobQueue(modules)
    
    def __enter__(self):
        self.local_queue.__enter__()
        
        self.session = drmaa.Session()
        
        self.session.initialize()
    
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.local_queue.__exit__(exc_type, exc_value, traceback)
        
        self.session.control(drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
    
        self.session.exit()
    
    def create(self, ctx, name, sent, temps_dir):
        return DrmaaJob(ctx, name, sent, temps_dir, self.modules, self.native_spec, self.session)
    
    def send(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            self.local_queue.send(ctx, name, sent, temps_dir)
        
        else:
            self.jobs[name] = self.create(ctx, name, sent, temps_dir)
    
    def wait(self):
        while True:
            if not self.local_queue.empty:
                name = self.local_queue.wait(immediate=True)
                
                if name is not None:
                    self.name_islocal[name] = True
                    
                    return name
                
            while True:
                for name, job in self.jobs.iteritems():
                    if job.finished:
                        return name
          
            time.sleep(1)
    
    def receive(self, name):
        if self.name_islocal.pop(name, False):
            return self.local_queue.receive(name)
        
        job = self.jobs.pop(name)
        
        job.finalize()
        
        return job.received

    @property
    def length(self):
        return len(self.jobs) + self.local_queue.length
    
    @property
    def empty(self):
        return self.length == 0

class QsubError(object):
    pass

class ContextError(Exception):
    pass
