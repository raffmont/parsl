import parsl
from parsl import sandbox_app
from parsl.data_provider.files import File
import os

parsl.load()

project = "helloworld"


@sandbox_app()
def app1(project=project):
    return 'echo "A" > outA.txt'


@sandbox_app()
def app2(arg, project=project, inputs=[]):
    return 'sleep {};echo "This is a test"> out.txt'.format(arg)


@sandbox_app()
def app3(project=project, inputs=[], outputs=[]):
    return 'echo B:"{}" C:"{}" > {}'.format(inputs[0], inputs[1], outputs[0])


fA = app1()

fB = app2(5,
          inputs=[fA.workflow_schema + "/out.txt"]
          )

fC = app2(10,
          inputs=[fA.workflow_schema + "/out.txt"]
          )

fD = app3(
    inputs=[
        fB.workflow_schema + "/out.txt",
        fC.workflow_schema + "/out.txt"
    ],
    outputs=[File(os.path.join(os.getcwd(), 'output.txt'))]
)

with open(fD.outputs[0].result(), 'r') as f:
    print(f.read())
