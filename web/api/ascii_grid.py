from flask import Blueprint, request, jsonify
import logging
import sys
import netCDF4
import numpy as np
import os
from flask import Flask, request, redirect, url_for
from flask import current_app as app
from werkzeug.utils import secure_filename
import netCDF4
import numpy as np
from datetime import datetime
from netCDF4 import date2num, num2date


bp = Blueprint('ascii_grid', __name__)
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = set(['txt', 'asc'])


def create_netcdf_file_by_stream(f):
    f = f.split('\n')
    ncols = int(f.pop(0).split('\t')[1])
    nrows = int(f.pop(0).split('\t')[1])
    xllconer = float(f.pop(0).split('\t')[1])
    yllconer = float(f.pop(0).split('\t')[1])
    cellsize = float(f.pop(0).split('\t')[1])
    NODATA_value = int(f.pop(0).split('\t')[1])
    logger.info(f'Meta: {ncols}, {nrows}, {xllconer}, {yllconer}, {cellsize}, {NODATA_value}')

    data = np.empty(shape=(nrows, ncols))
    lineNo = 0
    for line in f:
        if len(line):
            data[lineNo, :] = [float(i) for i in line.split(' ')]
            lineNo += 1

    ncfile = netCDF4.Dataset('/tmp/grid_data.nc', mode='w')
    logger.info(ncfile)
    lat_dim = ncfile.createDimension('latitude', nrows)  # Y axis
    lon_dim = ncfile.createDimension('longitude', ncols)  # X axis
    time_dim = ncfile.createDimension('timestamp', None)
    logger.info("Dimentions: %s", ncfile.dimensions.items())

    ncfile.moduleId = 'HEC-HMS'
    ncfile.valueType = 'Scalar'
    ncfile.parameterId = 'O.Precipitation'
    ncfile.locationId = 'wdias_hanwella'
    ncfile.timeseriesType = 'External_Historical'
    ncfile.timeStepId = 'each_hour'

    lat = ncfile.createVariable('latitude', np.float32, ('latitude',))
    lat.units = "Kandawala"
    logger.info("latitude: %s", lat)
    lon = ncfile.createVariable('longitude', np.float32, ('longitude',))
    lon.units = "Kandawala"
    time = ncfile.createVariable('timestamp', np.float64, ('timestamp',))
    time.units = "days since 1970-01-01 00:00"
    val = ncfile.createVariable('value', np.float32, ('timestamp', 'latitude', 'longitude',))
    val.units = 'O.Precipitation'

    # Write data
    lat[:] = (yllconer + cellsize * nrows) - cellsize * np.arange(nrows)
    lon[:] = xllconer + cellsize * np.arange(ncols)
    val[0, :, :] = data

    dates = [datetime(2017, 5, 20, 00, 15, 00)]
    logger.info('Date: %s', dates)
    times = date2num(dates, time.units)
    logger.info('Times: %s %s', times, time.units)  # numeric values
    time[:] = times

    # first logger.info() the Dataset object to see what we've got
    logger.info(ncfile)
    # close the Dataset.
    ncfile.close()
    logger.info('Dataset is closed!')


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@bp.route("/import/ascii-grid/binary/<string:timeseries_id>", methods=['POST'])
def upload_file(timeseries_id):
    # check if the post request has the file part
    if 'file' not in request.files:
        f = request.stream
        logger.info(f)
        for line in f.readlines():
            logger.info('l >> %s', line)
        return 'OK', 200
    file = request.files['file']
    # if user does not select file, browser also
    # submit a empty part without filename
    if file.filename == '':
        return 'No selected file', 400
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        f = file.read()
        # logging.info(f.decode('utf-8'))
        create_netcdf_file_by_stream(f.decode('utf-8'))
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return "OK", 200
