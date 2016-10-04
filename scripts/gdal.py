#!/usr/bin/python

from utils.utils import execute, sysexecute

class GDAL(object):
    def __init__(self):
        self.wm = '1024'
        self.rmethod = 'nearest'
        self.multithread = True
        self.overwrite = True
        self.trX = '0.30'
        self.trY = '-0.30'

    # TODO: add remaining options for gdaladdo
    def addOverviews(self, file, scales, configs=False):
        command = ['gdaladdo']
        command.append('-r')
        command.append(self.rmethod)
        command.append('-clean')
        if configs:
            command.append(configs)
        command.append(file)
        for scale in scales:
            command.append(scale)
        sysexecute(command)

    def warp(self, inputf, outputf, t_srs):
        command = ['gdalwarp']
        command.append('-r')
        if self.rmethod == 'nearest':
            command.append('near')
        else:
            command.append(self.rmethod)
        command.append('-wm')
        command.append(self.wm)
        if self.multithread:
            command.append('-multi')
        command.append('-t_srs')
        command.append(t_srs)
        if self.overwrite:
            command.append('-overwrite')
        command.append(inputf)
        command.append(outputf)
        sysexecute(command)

