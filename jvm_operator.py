from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults


class JVMOperator(BashOperator):
    template_fields = ['correlation_id', 'jar', 'options']

    @apply_defaults
    def __init__(
            self,
            correlation_id,
            jar,
            options,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args, **kwargs):
        super(JVMOperator, self).__init__(bash_command='', *args, **kwargs)
        self.correlation_id = correlation_id
        self.jar = jar
        self.options = options
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        try:
            self.log.info("| correlationId={0} | op=jar-execution | status=OK | desc=Started processing task '{1}'".
                          format(self.correlation_id, self.task_id))
            bucket_helper = GoogleCloudBucketHelper(self.gcp_conn_id, self.delegate_to)
            self.jar = bucket_helper.google_cloud_to_local(self.jar)
            command = 'java -jar {0}'.format(self.jar)

            if self.options is not None:
                for attr, value in self.options.iteritems():
                    command += " -" + attr + " \"" + value + "\""
            self.bash_command = command

            super(JVMOperator, self).execute(context)

            self.log.info("| correlationId={0} | op=jar-execution | status=OK | desc=Completed processing task '{1}'".
                          format(self.correlation_id, self.task_id))
        except Exception as exception:
            self.log.error(
                "| correlationId={0} | op=jar-execution | status=KO | desc=Failed to process the task '{1}' with exception '{2}'".
                    format(self.correlation_id, self.task_id, exception))
            raise exception
