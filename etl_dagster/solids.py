from dagster import( 
    solid,
    String,
    Int
)

@solid
def print_hello(context) -> String:
    print("Hello")
    return "Hello"


@solid
def add_two(context, number: Int) -> Int:
    return number + 2


@solid
def send_number(context) -> Int:
    return 5