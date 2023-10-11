import time
import random


def prod(x, y, z):  # 다변수 함수.... (매개변수 3개)
    return x * y * z


print(prod(10)(20)(30))

# def multiply_random():
#     result = random.randrange(10)*random.randrange(10)
#     time.sleep(3)
#     print(result)
#     return result


# multiply_random()

# x = 7
# print(x)
# print(type(x))


# def announce(text): print(text)


# announce("hello")


# def cube(x):
#     """triple function"""
#     return x*x*x


# print(cube(4))
# print(cube.__doc__)
# x = [1, 2, 3, 4]
# y = ('a', 'b', 'c', 'd')


# def printsome(a, b, c, d):
#     print(a)
#     print(b)
#     print(c)
#     print(d)


# printsome(*y)


# def printany(*args):
#     for arg in args:
#         print(arg)


# printany(10, 20)


# def funcname(**kwargs):
#     for kw, arg in kwargs.items():
#         print(kw, ': ', arg, sep="")


# print(funcname())
# print(funcname(a=10))
# print(funcname(a=10, b=20))

# def square2(x): return x*x


# print((lambda x: x*x)(3))

# print(square2(2))
