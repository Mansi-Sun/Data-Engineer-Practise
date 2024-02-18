#Homework code
## 1. Use a generator

Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

**Answer the following questions:**

- **Question 1: What is the sum of the outputs of the generator for limit = 5?**
```
def square_root_generator(limit):
    n=1
    while n<=limit:
        yield n**0.5
        n +=1

limit=5
generator=square_root_generator(limit)
total=sum(generator)
print(total)
```
- **Question 2: What is the 13th number yielded**
```
from itertools import islice

def square_root_generator(limit):
    n=1
    while n<=limit:
        yield n**0.5
        n +=1

limit=13
generator=square_root_generator(limit)

thirteenth_result=next(islice(generator,12,13))
print(thirteenth_result)
    
```

