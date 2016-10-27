#!/usr/bin/env python2

from math import floor
from random import random

def generateZipfian(input_fn, a, words):

  b = 2**(a-1)

  # populate input dictionary
  dict = [ ]

  with open(input_fn) as input_file:
    for line in input_file:
      dict.append(line.rstrip())

  # generator
  while True:
    ret = ''

    for i in xrange(words):

      while True:
        u = random()
        v = random()
        x = u**(-1/(a-1))
        t = (1+1/x)**(a-1)

        if (v*x*(t-1)/(b-1)) <= (t/b) and x < len(dict):
          break;

      ret += dict[int(floor(x))]

    yield ret

# ---- Main function ----

if __name__ == '__main__':

  z = generateZipfian('/usr/share/dict/words',
                      a = 1.1, words = 3);

  for i in xrange(10**6):
    print(next(z))

