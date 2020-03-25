from dagster import pipeline

from .solids import print_hello, add_two, send_number

# Define etl pipelines composed of solids
@pipeline
def hello_pipeline():
    print_hello()


@pipeline
def adder_pipeline():
    num = send_number()
    add_two(num)


pipelines = {
    'hello_pipeline': lambda : hello_pipeline,
    'adder_pipeline': lambda : adder_pipeline
}