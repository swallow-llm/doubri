import matplotlib.pyplot as plt
import numpy as np

def p(s, b, r):
    return 1 - (1 - s ** b) ** r

s = np.linspace(0, 1, 500)

fig, ax = plt.subplots()
ax.plot(s, p(s, 8, 14), ls="dotted", label='$b=8, r=14$')
ax.plot(s, p(s, 20, 40), ls="dashed", label='$b=20, r=40$')
ax.plot(s, p(s, 20, 450), ls="solid", label='$b=20, r=450$')
ax.set_xlabel('Jaccard coefficient ($s$)')
ax.set_ylabel('Probability to be recognized as duplicates ($p$)')
ax.grid()
ax.legend(loc='upper left')
plt.savefig('similarity.svg')
plt.show()