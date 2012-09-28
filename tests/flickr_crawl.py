#!/usr/bin/env python
import gevent.monkey
gevent.monkey.patch_all()
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation
from thrift_bench import random_string, setup, remove_table
import vision_data
import random
#import multiprocessing
import time

def setup():
    transport = TBufferedTransport(TSocket('api.picar.us', 9090))
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    return client


def main():
    #tags = ' animals   architecture   art   asia   australia   autumn   baby   band   barcelona   beach   berlin   bike   bird   birds   birthday   black   blackandwhite   blue   bw   california   canada   canon   car   cat   chicago   china   christmas   church   city   clouds   color   concert   dance   day   de   dog   england   europe   fall   family   fashion   festival   film   florida   flower   flowers   food   football   france   friends   fun   garden   geotagged   germany   girl   graffiti   green   halloween   hawaii   holiday   house   india   instagramapp   iphone   iphoneography   island   italia   italy   japan   kids   la   lake   landscape   light   live   london   love   macro   me   mexico   model   museum   music   nature   new   newyork   newyorkcity   night   nikon   nyc   ocean   old   paris   park   party   people   photo   photography   photos   portrait   raw   red   river   rock   san   sanfrancisco   scotland   sea   seattle   show   sky   snow   spain   spring   square   squareformat   street   summer   sun   sunset   taiwan   texas   thailand   tokyo   travel   tree   trees   trip   uk   unitedstates   urban   usa   vacation   vintage   washington   water   wedding   white   winter   woman   yellow   zoo '.strip().split()
    tags = ['Pyramids Of Giza', 'Great Wall Of China', 'Terracotta Warriors', 'Statue Of Liberty', 'Edinburgh Castle', 'Stirling Castle', 'Empire State Building', 'Stonehenge', 'Blackpool Tower', 'London Bridge', 'Tower Bridge', 'Buckinghampalace', 'Sphinx', 'Eiffle Tower', 'Arc Du Triomph', 'Louvre', 'Cristo Redentor', 'CN Tower', 'Norte Dame', 'River Nile', 'Mount Rushmore', 'Pentagon', 'White House', 'Lincoln Memorial', 'Grand Canyon', 'Leaning Tower Of Piza', 'Easter Island Heads', 'Niagara Falls', 'Abbey Road', 'Ayers Rock', 'Evangeline Oak', 'Lone Cyprus', 'Golden Gate Bridge', 'Colosseum', 'Taj Mahal', 'Santorini']
    client = setup()
    random.shuffle(tags)
    flickr = vision_data.Flickr(max_iters=1)
    #remove_table(client, 'flickr')
    #client.createTable('flickr', [ColumnDescriptor('metadata:'), ColumnDescriptor('images:')])
    while True:
        for tag in tags:
            mutations = []
            try:
                for url_m, metadata in flickr.image_class_meta_url(tag):
                    mutations.append(BatchMutation(row=url_m, mutations=[Mutation(column='metadata:%s' % x, value=y.encode('utf-8'))
                                                                         for x, y in metadata.items()]))
            except Exception, e:
                print(e)
                continue
            st = time.time()
            client.mutateRows('flickr', mutations)
            if mutations:
                print((tag, (time.time() - st) / len(mutations), len(mutations)))
            else:
                print((tag, 0., len(mutations)))

def scanner(client, table, columns=None, per_call=1, start_row=''):
    try:
        sc = client.scannerOpen(table, start_row, columns if columns else [])
        if per_call == 1:
            scanner = lambda : client.scannerGet(sc)
        else:
            scanner = lambda : client.scannerGetList(sc, per_call)
        while True:
            outs = scanner()
            if outs:
                for out in outs:
                    yield (out.row, dict((x, y.value)
                                         for x, y in out.columns.items()))
            else:
                break
    finally:
        client.scannerClose(sc)


def display():
    client = setup()
    for x in scanner(client, 'flickr', ['metadata:title']):
        print(x)

if __name__ == '__main__':
    gevent.joinall([gevent.spawn(main) for x in range(30)])
