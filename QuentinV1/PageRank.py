#! /usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class PageRank(MRJob):
    global initial_weight
    global cout
    global nb_iter
    cout=0.15
    nb_iter=10

    def recup_resend(self, _, line):
        line = line.split('\t')
        try:
            yield line[0], line[1]
        except:
            pass

    def recup_and_list(self, key, values):
        yield key, list(values)
        yield "nb_node", 1

    def set_w0(self, key, values):
        global initial_weight
        if key == 'nb_node':
            initial_weight = 1 / sum(values)
        else:
            yield key, *list(values)

    def set_nodew0(self, key, values):
        yield key, [{'weight': initial_weight}, list(values)]

    def nextrank1(self, key, values):
        w, liste = values
        for out in liste:
            yield out, w['weight'] / len(liste)
        yield key, values

    def nextrank2(self, key, values):
        yield key, list(values)

    def nextrank3(self, key, values):
        values = values
        L = []
        val = 0
        for i in values:
            if type(i) == type(L):
                L = i
            else:
                val += i
        if len(L)!=0:
            L[0]['weight'] = cout * initial_weight + (1 - cout) * val
            yield key, L

    def sortie(self,key,values):
        yield key, values[0]['weight']
        
    def steps(self):
        return [
            MRStep(mapper=self.recup_resend, reducer=self.recup_and_list),
            MRStep(reducer=self.set_w0),
            MRStep(mapper=self.set_nodew0)]+[ MRStep(mapper=self.nextrank1, reducer=self.nextrank2),
            MRStep(mapper=self.nextrank3)]*nb_iter+[MRStep(mapper=self.sortie)]
        


if __name__ == '__main__':
    PageRank.run()