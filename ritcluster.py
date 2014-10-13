from fabric.api import env, run, local, task, cd, settings, hosts, execute
import os
import re
import uuid
import json
import time
import getpass
import argparse

def parse_job_id(s):
    try:
        return re.findall("Submitted batch job (.*)", s)[0]
    except KeyError:
        return ''

sbatch_script = \
"""#!/bin/bash
#!/bin/sh
#SBATCH -J ipython
#SBATCH -n {n}
#SBATCH --output {pwd}/engines.out

echo "" > {pwd}/engines.out

srun {python_basedir}/ipengine --work-dir="{pwd}" --cluster-id='{uuid}' --timeout=30
"""
env['user'] = getpass.getuser()
env['uuid'] = str(uuid.uuid4())[:8]
env['tmux_session'] = "ipcluster-{uuid}".format(**env)
env["tmux_window"] = 'ipcontroller'
env['python_basedir'] = '/network/rit/home/ck573381/.pyenv/versions/2.7.8/bin'

@task    
def kill_cluster():

    with open(".job_id", "r") as fh:
        launching_env = json.loads(fh.read())

    with settings(warn_only=True):        
        run('{python_basedir}/python -c "from IPython.parallel import Client; Client(cluster_id=\'{uuid}\')[:].shutdown(hub=True)"'.format(**launching_env))
        run('tmux kill-session -t {tmux_session}'.format(**launching_env))

        # Failsafe,  just in case we killed the hub and can't reach the engines anymore
        
        local("rm .job_id")
        run('scancel {job_id}'.format(**launching_env))

@task
def launch_controller():
    env['pwd'] = os.getcwd()
    env['cmd'] = "{python_basedir}/ipcontroller --ip='*' --work-dir='{pwd}' --cluster-id='{uuid}'".format(**env)
    
    with cd(env['pwd']):
        result = run('tmux new-session -d -s '
                     '{tmux_session} -n {tmux_window}'.format(**env),
                     warn_only=True)
        if result.failed:
            run('tmux send-keys -t {tmux_session}:{tmux_window} C-c C-c'.format(**env))

        run('tmux send-keys -t {tmux_session}:{tmux_window} "{cmd}" Enter'
            .format(**env))

        run("tmux new-window -t {tmux_session} \
        'watch --interval=2 tail -n 40 engines.out '".format(**env))

@task
def launch_engines(n=4):
    env['pwd'] = os.getcwd()
    env['n'] = n

    with cd(env['pwd']):
        run('echo "' + sbatch_script.format(**env) + '" > /tmp/ipengines_{user}.sh'.format(**env))
        env['job_id'] = parse_job_id(run("sbatch /tmp/ipengines_{user}.sh".format(**env)))
        
        with open(".job_id", "w") as fh:
            fh.write(json.dumps(env))
        

def main():
    parser = argparse.ArgumentParser(description="To Write")

    sp = parser.add_subparsers(dest="sub_parser", help="To Write")

    sp_start = sp.add_parser("start",
                             help="convert a document to different types")
    sp_start.add_argument("nodes",
                        type=int,
                        default=4,
                        help="directories to save documents to")

    sp_kill = sp.add_parser("kill",
                             help="convert a document to different types")
    
    args = parser.parse_args()
    if args.sub_parser == "start":
        env.hosts = ['localhost']
        execute(launch_controller)

        # Sleep for a hot second (this should be better)
        time.sleep(2)

        execute(launch_engines, args.nodes)
    else:
        env.hosts = ['localhost']
        execute(kill_cluster)


if __name__ == "__main__":
    main()
