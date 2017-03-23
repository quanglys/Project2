# -*- coding: utf-8 -*-
import argparse

def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-type', dest='type',  help='Type of message', type=int, nargs = 1)
    parser.add_argument('-name', dest='name', help='Name of monitor node', type=str, nargs = 1)
    parser.add_argument('-data', dest='data', help='array of partial value', type=str, nargs = 1)
    parser.add_argument('-top', dest='top', help='top nodes', type=str, nargs=1)
    parser.add_argument('-eps', dest='eps', help='Epsilon', type=int, nargs = 1)
    parser.add_argument('-bound', dest='bound', help='current value of the k-th best object', type=int, nargs = 1)
    parser.add_argument('-value', dest='value', help = 'value from monitor node', type=int, nargs = 1)
    parser.add_argument('-ses', dest='session', help='ID of current session', type=int, nargs=1)
    parser.add_argument('-k', dest='k', help='The number of elements in top', type=int, nargs=1)
    parser.add_argument('-band', dest='band', help='limitation of bandwidth', type=int, nargs=1)
    parser.add_argument('-f', dest='f', help='nodes make invalid constrains', type=str, nargs=1)
    parser.add_argument('-border', dest='border', help='border value', type=int, nargs=1)
    return parser
