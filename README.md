# apache-airflow-jvm-operator
Helps to run JVM languages like Java, Scala, Kotlin, Clojure in [Apache Airflow](https://airflow.apache.org)

**Example usage:**

```
jvm_task = JVMOperator(
    task_id='jvm_task',
    correlation_id='123456',
    jar='GCS_PATH/JAR_NAME.jar',
    options={
        'option1': 'value1',
         'option2': 'value2',
    })
```
