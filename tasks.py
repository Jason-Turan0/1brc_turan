from invoke import task
import os

@task
def run(context):
    context.run(f'python main.py')
 
@task
def chunk(context):
    context.run(f'python chunk_file.py')

@task
def test(context):
    context.run(f'cd ants_ai {sep} python -m unittest')

@task
def start_profiler(context):
    context.run('snakeviz results/1brc_turan_0.profile')
