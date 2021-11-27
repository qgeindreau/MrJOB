#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRPageRank(MRJob):
    
    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg('--nodes', default=None, type=int)

    def get_links(self, _, line):
        d = json.loads(json.dumps(line))
        node = next(iter(d))
        try:
            yield node, {'links': d[int(node)], 'pr': 1 / self.options.nodes}
        except:
            pass

    def map_task(self, node, outlink_list_pr):
        outlink_list = outlink_list_pr['links']
        pr = outlink_list_pr['pr']

        if len(outlink_list) > 0:
            for outlink in outlink_list:
                yield outlink, pr / len(outlink_list)
        else:
            yield 'dangling', pr

        yield node, outlink_list

    def reduce_task(self, node, list_pr_or_urls):
        if node == 'dangling':
            m = sum(list_pr_or_urls)
            for n in range(1, int(self.options.nodes) + 1):
                yield str(n), m
        else:
            outlink_list = {}
            pr = 0

            for pr_or_urls in list_pr_or_urls:
                if type(pr_or_urls) == dict:
                    outlink_list = pr_or_urls
                else:
                    pr += pr_or_urls

            yield node, {'links': outlink_list, 'pr': pr}

    def update_task(self, node, list_pr_or_urls):
        outlink_list = {}
        pr = 0
        ip = 0

        for pr_or_urls in list_pr_or_urls:
            if type(pr_or_urls) == dict:
                outlink_list = pr_or_urls['links']
                pr = pr_or_urls['pr']
            else:
                ip = pr_or_urls

        pr = c * pr + c * (ip / self.options.nodes) + (
                (1 - c) / self.options.nodes)

        yield node, {'links': outlink_list, 'pr': round(pr, 4)}

    def steps(self):
        return ([MRStep(mapper=self.get_links)] +
                [MRStep(mapper=self.map_task,
                        reducer=self.reduce_task),
                 MRStep(reducer=self.update_task)] * iterations)


if __name__ == '__main__':
    c = 0.15
    iterations = 10
    MRPageRank.run()
