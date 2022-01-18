import sys
import re
import json
from itertools import chain
from urllib.request import urlopen
from urllib.parse import urlparse
from datetime import datetime

import pandas
import matplotlib.pyplot as plt
from lxml.html import parse


class Iter():
    def __init__(self, it):
        self.it = it

    def pipe(self, fn, *args, **kwargs):
        return Iter(fn(self.it, *args, **kwargs))

    def mapall(self, *fns):
        return Iter(tuple(fn(y) for y, fn in zip(x, fns)) for x in self.it)

    def map(self, fn, *args, **kwargs):
        return Iter(fn(x, *args, **kwargs) for x in self.it)

    def mapnth(self, nth, fn, *args, **kwargs):
        return Iter(x[:nth] + (fn(x[nth], *args, **kwargs),) + x[nth+1:] for x in self.it)

    def mapfst(self, fn, *args, **kwargs):
        return self.mapnth(0, fn, *args, **kwargs)

    def mapsnd(self, fn, *args, **kwargs):
        return self.mapnth(1, fn, *args, **kwargs)

    def starmap(self, fn, *args, **kwargs):
        return Iter(fn(*x, *args, **kwargs) for x in self.it)

    def dictmap(self, fn, *args, **kwargs):
        return Iter(fn(*args, **kwargs, **x) for x in self.it)

    def fan(self, fns, *args, **kwargs):
        return Iter(tuple(fn(x, *args, **kwargs) for fn in fns) for x in self.it)

    def flatten(self):
        return Iter(chain.from_iterable(self.it))

    def filter(self, pred, *args, **kwargs):
        return Iter(x for x in self.it if pred(x, *args, **kwargs))

    def starfilter(self, pred, *args, **kwargs):
        return Iter(x for x in self.it if pred(*x, *args, **kwargs))

    def compute(self):
        return list(self.it)


noticia_url = "https://www.campinas.sp.gov.br/export/export-noticias-integra-newportal.php?id={id}"

def parse_title(title):
    match = re.search(r"\d+(,\d+)?%", title)
    if match:
        return float(".".join(match.group(0)[:-1].split(",")))
    else:
        return None

def parse_date(url):
    parts = urlparse(url)
    query = dict(item.split("=") for item in parts.query.split(";"))
    new_url = noticia_url.format(**query)
    data = json.load(urlopen(new_url))
    date = data[0].get("not_dtinicial")
    return datetime.strptime(date, "%d/%m/%Y - %H:%M")


tree = parse(urlopen("https://covid-19.campinas.sp.gov.br/noticias"))
df = (
    Iter(tree.xpath("//div[@class='titulo']/a"))
    .map(lambda el: (el.get("href"), el.text_content().lower()))
    .starfilter(lambda _, title: "leito" in title and "ocupado" in title)
    .mapall(parse_date, parse_title)
    .starfilter(lambda _, match: match is not None)
    .pipe(list)
    .pipe(pandas.DataFrame, columns=["date", "occupancy"])
    .pipe(pandas.DataFrame.set_index, "date")).it

df.to_csv("data.csv")

last = df.loc[df.index.max()].occupancy

plt.figure(figsize=(15, 15))
ax = df.plot()
ax.set_ylim(0, 100)
plt.axhline(last, color="red")
plt.title("Taxa de ocupacão de leitos de UTI em Campinas")
plt.grid(True)
plt.tight_layout()
plt.savefig("data.png")
# plt.show()
