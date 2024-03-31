from invoke import task
import os

@task
def run(context):
    context.run(f'python main.py')


@task
def test(context):
    context.run(f'cd ants_ai {sep} python -m unittest')

@task
def start_profiler(context):
    context.run('snakeviz ants_example.profile')
