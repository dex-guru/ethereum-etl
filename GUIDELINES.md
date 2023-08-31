# Project Guidelines

## Favor Class Composition Over Inheritance
* **Reduced Coupling**: Using composition over inheritance reduces coupling between classes, making the codebase easier to maintain and understand.
* **Explicit Dependencies**: It's clear what dependencies are required by a class.
* **Increased Flexibility**: Composition allows dynamic behavior swapping, unlike inheritance which is static.
* **Ease of Testing**: Testing composed objects is often simpler because you can isolate components.
    
```python
# Good: Using Composition
class Engine:
    def start(self):
        print("Engine started")

class Car:
    def __init__(self, engine):  # Clearly defined dependencies
        self.engine = engine
    
    def start(self):
        self.engine.start()

# Bad: Using Inheritance
class Car(Engine):
    pass
```

## Use Base Classes or Protocols for Interfaces
* **Explicitness**: abc.ABC makes it clear that a class is intended to be abstract.
* **Abstract methods or properties must be defined** in base classes.
* **Protocols**: More flexible, facilitates duck typing. Define protocols where the interface is consumed, not provided.
* **MyPy**: Use MyPy for type-checking, especially when using Protocols.
* **Signatures** of methods must exactly match the base class or protocol.
* **Only inherit once from a base class**. Don't use multiple inheritance. 

For choosing between `abc` and Protocols, use `abc` when you want to enforce 
that implementers inherit the base class. Protocols are more flexible and suitable for 
loosely-coupled architectures.

```python
# Good: Using abc.ABC
from abc import ABC, abstractmethod

class Shape(ABC):
    @abstractmethod
    def draw(self, x, y):
        ...
        
class Circle(Shape):
    def draw(self, x, y):
        print(f"Drawing circle {self} at {x}, {y}")


# Good: Using Protocols
from typing import Protocol

class Drawable(Protocol):
    def draw(self, x: int, y: int) -> None:
        ...
        
def render(drawable: Drawable, x: int, y: int) -> None:
    drawable.draw(x, y)
```

## Encapsulate logic in pure functions

* Use pure functions for logic: Input -> Logic -> Output.
* Keep I/O tasks separate.
* Chain I/O and pure functions linearly.
* **Benefits**:
  * Easier testing without mocks.
  * Easier to reuse logic. 
  * Data fetching and processing can be parallelized.

```python
# Bad: Mixing I/O and logic
def process_api_data():  # needs API to be mocked for testing
    api = ExternalServiceAPI()               # unnecessary coupling
    data = api.get_data()                    # i/o
    processed = data.strip() + " processed"  # logic
    api.write_data(processed)                # i/o
    


# Good: Separating I/O and logic
def process(data):  # Pure function with logic. Can be tested easily.
    return data.strip() + " processed"

def main(): # Chaining I/O and logic
    api = ExternalServiceAPI()  # initialize once in main
    data = api.get_data()
    processed = process(data)   # no logic here, just chaining
    api.write_data(processed)
```

## Avoid `kwargs`

* Make function signatures explicit
* Easier type-checking for static analysis tools

```python
# Bad
def greet(**kwargs):
    print(f"Hello, {kwargs['name']}. You are {kwargs['age']} years old.")

# Good
def greet(name, age):
    print(f"Hello, {name}. You are {age} years old.")
```

## Function signature

* A Function's logic should be clear from its signature (and docstring, rearly).
* The user of a function should not be forced to read the function source to understand what it does.

```python
# Bad
def calc(a, b):
    # calculated the area of a rectangle whose sides are a and b
    return a * b
    
# Good
def rectangle_area(width: int, height: int) -> int:
    return width * height
```

## Introduce Abstraction After Patterns Emerge

* Wait until you've written similar code 2-3 times.
* Introduce abstraction only when you have a clear idea of how to generalize the code.
* Make sure the abstraction is not too specific or too general.
* Make sure the abstraction doesn't duplicate existing abstractions.

## Merge Requests

* Split refactoring and new features into different MRs.
* Help reviewers: highlight key code sections, summarize changes and objective in MR description.
* Use `Draft` status until ready for review.
* Remove `Draft` status after CI pipeline succeeds.

## Optimizations

* Measure, don't guess.
* Optimize the slowest parts first.
* Measure again after optimization. Validate that the optimization was effective. Then merge.
