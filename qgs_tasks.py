import os
from qgis.core import *
from qgis.gui import *
from PyQt4.QtCore import QFileInfo, QSize, QSizeF
from PyQt4.QtGui import QImage, QColor, QPainter, QPrinter

#Adds the shapefiles in list to a qgs project
def make_qgs():
    '''Gets list of shapefiles from wd. Adds them to base.qgs.
    Returns bounding box coords for project.'''

    #Loads project
    wd = os.getcwd()
    f_l = [f for f in os.listdir(wd) if f.split('.')[-1] == 'shp']
    qgs = '/home/bug/Documents/GPS/base.qgs'
    p = QgsProject.instance()
    p.read(QFileInfo(qgs))

    #Sets up max/min coords lists for extent
    lats, lons = [], []

    #Loads layers
    for f in f_l:
        layer = QgsVectorLayer(f, f.split('.')[0], "ogr")
        QgsMapLayerRegistry.instance().addMapLayer(layer)
        if not layer.isValid():
            print "{f} not added as layer".format(f=f)
        lats.append(layer.extent().yMaximum())
        lats.append(layer.extent().yMinimum())
        lons.append(layer.extent().xMaximum())
        lons.append(layer.extent().xMinimum())

    xmin, xmax = min(lons), max(lons)
    ymin, ymax = min(lats), max(lats)
    f = "Session_map.qgs"
    p.write(QFileInfo(f))
    return f, xmin, ymin, xmax, ymax

make_qgs
