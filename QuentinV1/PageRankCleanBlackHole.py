#! /usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class PageRank(MRJob):
    global initial_weight
    global cout
    global nb_iter
    global NBlackHole
    global clearing_iterator
    clearing_iterator = 0
    cout=0.15
    nb_iter=10
    NBlackHole=set()
    ambigousBlackHole=set()
    def prep_clr(self,_,line):
        global NBlackHole
        NBlackHole=set()
        yield 1,line
        
        
    def init_clear(self, _, line):
        line = line.split('\t')
        try:
            NBlackHole.add(line[0])
            yield _,line[0]+"\t"+line[1]
        except: pass
        
    def clear(self, _, line):
        line = line.split('\t')
        if line[1] in NBlackHole:
            yield _,line[0]+"\t"+line[1]
            
            
            
            
    def recup_resend(self, _, line):
        '''On fait un peut de ménage, ca fait disparaitre les lignes vides et non conforme'''
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
        return [MRStep(mapper=self.prep_clr),MRStep(mapper=self.init_clear),MRStep(mapper=self.clear)]*clearing_iterator+[
            MRStep(mapper=self.recup_resend, reducer=self.recup_and_list),
            MRStep(reducer=self.set_w0),
            MRStep(mapper=self.set_nodew0)]+[ MRStep(mapper=self.nextrank1, reducer=self.nextrank2),
            MRStep(mapper=self.nextrank3)]*nb_iter+[MRStep(mapper=self.sortie)]
        


if __name__ == '__main__':
    PageRank.run()