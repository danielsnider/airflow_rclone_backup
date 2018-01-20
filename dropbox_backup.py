from IPython import embed


from stat import S_IFDIR, ST_CTIME, ST_MODE, ST_MTIME
import subprocess
import os, sys, time

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.email import send_email


# Adapted from: https://stackoverflow.com/a/46819184/1631623
# Adapted from: https://github.com/geosolutions-it/evo-odas/wiki/Airflow---about-subDAGs,-branching-and-xcom


default_args = {
    'provide_context': True,
    'email_on_failure': True,
    'email': ['danielsnider12@gmail.com'],
}


dag = DAG(
  dag_id='dropbox_backup',
  start_date=datetime(2017, 10, 26),
  schedule_interval=timedelta(1),
  default_args=default_args
)


def _rclone_copy(**context):
    ti = context['ti']
    new_folders = ti.xcom_pull(task_ids='new_folders_check',key='new_folders')
    print "Folders to be backed up: %s " % new_folders

    for folder in new_folders:
        folder_name = os.path.basename(os.path.normpath(folder))
        cmd = 'rclone copy "%s" dropbox:"/Backup/OPRETTA/Operetta Raw Data/Mammalian cells/%s" -v' % (folder, folder_name)
        print "Executing command: %s" % cmd
        p = subprocess.Popen(cmd, shell=True)
        p.wait()
        print "Return code: %s" % p.returncode
        if p.returncode != 0:
            raise Exception('Rclone copy failed!')

    # NOTE(Dan): This section uses Popen WITHOUT shell=True but because of the 
    #            space in the path it would not work.
    # 
    # cmd = ['rclone','copy', folder, 'dropbox:Daniel/%s' % folder_name, '-v']
        # print "Executing command: %s" % ' '.join(cmd)
        # cmd = ['rclone','copy', folder, "dropbox:'/Backup/OPRETTA/Operetta","Raw", "Data/%s'" % folder_name, '-v']
        # p = subprocess.Popen(cmd)
        

rclone_copy = PythonOperator(
    task_id='rclone_copy', 
    python_callable=_rclone_copy,
    trigger_rule="all_done",
    dag=dag)


def _send_email(**context):
    ti = context['ti']
    msg = ti.xcom_pull(task_ids='rclone_copy',key='number_of_files')
    new_folders = ti.xcom_pull(task_ids='new_folders_check',key='new_folders')

    num_folders = len(new_folders) if new_folders else 0
    status = 'Backup Succeeded' if num_folders else 'Nothing New'

    html_content = """
        <h2>SickKids Dropbox Backup Notfication</h2>
        <p>Status: {status}</p>
        <p>Date: {date}</p>
        <p>Number of folders backed up: {num_folders}</p>
        <p>Folders: {folders}</p>
    """.format(status=status,
               date=context['ds'],
               num_folders=num_folders,
               folders=new_folders)

    subject = 'SickKids Airflow: %s (%s)' % (status, context['ds'])

    print "Sending email: \n\n\tSubject: %s\n%s" % (subject, html_content)

    send_email(
            to=[
                'danielsnider12@gmail.com'
            ],
            subject=subject,
            html_content=html_content
        )


email = PythonOperator(
    task_id='send_email', 
    python_callable=_send_email,
    trigger_rule="all_done",
    dag=dag)


def _new_folders_check(**context):
    new_folders = []
    exec_date = context['execution_date']
    # backup_dir = '/home/dan/temp'
    backup_dir = '/mnt/hgfs/Z/OPRETTA/Operetta Raw Data/Mammalian cells/'
    backup_paths = [os.path.join(backup_dir, d) for d in os.listdir(backup_dir)]
    backup_folders = filter(os.path.isdir, backup_paths)
    folder_stats = [(os.stat(path), path) for path in backup_folders]
    folder_dates = [(stat[ST_MTIME], path) for stat, path in folder_stats]
    #NOTE: on Windows `ST_CTIME` is a creation date 
    #  but on Unix it could be something else
    #NOTE: use `ST_MTIME` to sort by a modification date

    for folder_date, path in sorted(folder_dates):
        folder_date = datetime.strptime(time.ctime(folder_date),'%a %b %d %H:%M:%S %Y')
        folder_date_difference = folder_date - exec_date
        if timedelta(0) < folder_date_difference < timedelta(1):
            # one day in the future (less than 0 would be the past)
            new_folders.append(path)

    print 'new_folders:  ', new_folders

    task_instance = context['task_instance']
    task_instance.xcom_push(key='new_folders', value=new_folders)

    if new_folders:
        return "rclone_copy"
    else:
        return "dummy"

new_folders_check = BranchPythonOperator(
    task_id='new_folders_check',
    python_callable=_new_folders_check,
    # depends_on_past=True,
    trigger_rule="all_done",
    dag=dag)


def _dummy(**context):
    new_folders = []

dummy = PythonOperator(
    task_id='dummy',
    python_callable=_dummy,
    trigger_rule="all_done",
    dag=dag)


new_folders_check >> rclone_copy >> email
new_folders_check >> dummy >> email
