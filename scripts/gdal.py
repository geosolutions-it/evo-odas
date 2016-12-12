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

    def addOverviews(self, file, scales, configs=False):
        command = ['gdaladdo']
        command.append('-r')
        command.append(self.rmethod)
        command.append('-clean')
        if configs:
            command.append(configs)
        command.append(file)
        for scale in scales:
            command.append(str(scale))
        sysexecute(command)

    def warp(self, inputf, outputf, t_srs, options=False):
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
        if options:
            command.append(options)
        command.append(inputf)
        command.append(outputf)
        sysexecute(command)

    def calc(self, outputf, datatype='Byte', filemap='-A myfile',
             bandsmap='--A_band=1', fformat='GTiff', creation_options=False,
             calc_expr='A+B'):
        command = ['gdal_calc.py']
        command.append('--type=' + datatype)
        command.append('--format=' + fformat)
        if creation_options:
            command.append(creation_options)
        command.append(filemap)
        command.append(bandsmap)
        command.append('--calc=' + calc_expr)
        command.append('--outfile=' + outputf)
        sysexecute(command)

    def merge(self, inputf, outputf, datatype='Byte', init=False,
              nodata_v=False, out_nodata_v=False, separate=False,
              fformat='GTiff', creation_options=False):
        command = ['gdal_merge.py']
        if datatype:
            command.append('-ot ' + datatype)
        if init:
            command.append('-init \"' + init + '\"')
        if nodata_v:
            command.append('-n ' + nodata_v)
        if out_nodata_v:
            command.append('-a_nodata ' + out_nodata_v)
        if separate:
            command.append('-separate')
        if fformat:
            command.append('-of ' + fformat)
        if creation_options:
            command.append(creation_options)
        if outputf:
            command.append('-o ' + outputf)
        for f in inputf:
            command.append(f)
        sysexecute(command)

