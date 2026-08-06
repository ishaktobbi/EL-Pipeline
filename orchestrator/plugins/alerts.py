# plugins/alerts.py
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook



def on_failure_slack_alert(context):
    """
    Airflow callback function triggered automatically on task failure.
    Extracts runtime context metadata and posts a formatted message to Slack.
    """
    ti = context.get('task_instance')   # retrieve the failed task instance from the context
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date') or ti.start_date
    log_url = ti.log_url
    exception = context.get('exception', 'No exception info available')

    # Fallback plain text for mobile/system notifications
    fallback_text = f"🚨 Task Failed: {dag_id}.{task_id}"

    # Rich Slack Block formatting
    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Airflow Task Failed!",
                "emoji": True
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* \n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:* \n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*Execution Time:* \n`{execution_date}`"},
                {"type": "mrkdwn", "text": f"*Log Link:* \n<{log_url}|View Airflow Logs>"}
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Exception:* \n```{str(exception)[:500]}```"
            }
        }
    ]

    # 1. Initialize the Hook with the Connection ID
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_alert_channel")

    # 2. Call .send() with text and blocks
    return slack_hook.send(
        text=fallback_text,
        blocks=slack_blocks
    )

def on_sla_miss_slack_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback triggered when an SLA deadline is breached.
    """
    # blocking_task_list     -> List of tasks that are causing delays to other tasks run due to SLA miss
    # blocking_tis           -> List of tasks instances (with context and id and..) that are causing delays to other tasks run due to SLA miss
    dag_id = dag.dag_id
    tasks_missed = task_list    # List of tasks that missed their SLA (airflow time to time and it can spot many tasks)
    blocking_tasks = [ti.task_id for ti in blocking_tis] if blocking_tis else ["Unknown"]  

    slack_blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "⚠️ Airflow SLA Missed!", "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* \n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Tasks Causing Delays Upstream:* \n`{', '.join(blocking_tasks)}`"},
                {"type": "mrkdwn", "text": f"*Tasks Exceeding SLA:* \n`{tasks_missed}`"},
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "The pipeline is taking longer than configured SLA limits. Downstream dashboards or reports may be delayed."
            }
        }
    ]

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_alert_channel')
    return slack_hook.send(text=f"⚠️ SLA Missed for DAG: {dag_id}", blocks=slack_blocks)