# pylint: disable=R0201
"""The :class:`geoutils.Image` class is a component of a
:class:`geoutils.Standard`: each standard presents a metadata component
and an image.  This class focuses on the image.

"""
__all__ = ["GeoImage"]

import Image
import numpy

from geosutils.log import log


class GeoImage(object):
    """:class:`geouitls.Image`

    """
    def extract_image(self, dataset, downsample=None):
        """Attempts to extract the image from the
        :attr:`geoutils.Standard.dataset` *dataset*

        Uses the :func:`osgeo.gdal.Dataset.GetRasterBand.ReadRaster`
        call::

            def ReadRaster(self,
                           xoff,
                           yoff,
                           xsize,
                           ysize,
                           buf_xsize = None,
                           buf_ysize = None,
                           buf_type = None,
                           band_list = None):

        * The ``xoff``, ``yoff,`` ``xsize`` and ``ysize arguments define
          the rectangle on the raster file to read.
        * The ``buf_xsize`` and ``buf_ysize`` values are the size of the
          resulting buffer

        For example, ``0, 0, 512, 512, 100, 100`` to read a 512x512
        block at the top left of the image into a 100x100 buffer
        (downsampling the image).

        **Args:**
            *dataset*: a :class:`gdal.Dataset` object generally
            obtained via a :func:`gdal.Open` operation

            *downsample*: column size of downsampled image

        **Returns:**
            On success, type ``string`` that contains
            :func:`osgeo.gdal.Dataset.GetRasterBand.XSize` x 4 bytes
            of raw binary floating point data.

            ``None`` otherwise.

        """
        def generate():
            """TODO

            """
            log.info('Preparing image stream ...')

            if dataset is None:
                log.warn('Extraction failed: dataset stream not provided')
            else:
                band = dataset.GetRasterBand(1)
                x_size = band.XSize
                y_size = band.YSize
                if downsample is not None:
                    (x_size, y_size) = self.scale((band.XSize, band.YSize),
                                                  downsample)

                log.debug('Raster count: %d' % dataset.RasterCount)

                result = None
                if dataset.RasterCount == 1:
                    result = band.ReadRaster(0, 0,
                                             band.XSize,
                                             band.YSize,
                                             buf_xsize=x_size,
                                             buf_ysize=y_size)
                elif dataset.RasterCount == 3:
                    result = self.extract_multiband_image(dataset,
                                                          band.XSize,
                                                          band.YSize,
                                                          x_size,
                                                          y_size)

                return result

        return generate

    def extract_multiband_image(self,
                                dataset,
                                x_size,
                                y_size,
                                buf_x_size,
                                buf_y_size):
        """TODO

        """
        image_rect = numpy.ndarray((buf_y_size, buf_x_size, 3), numpy.uint8)

        for i in range(3):
            band = dataset.GetRasterBand(i + 1)
            color = band.ReadRaster(0, 0,
                                    x_size,
                                    y_size,
                                    buf_x_size,
                                    buf_y_size)

            arr = numpy.fromstring(color, numpy.uint8)
            image_rect[:, :, i] = arr.reshape([buf_y_size, buf_x_size])

        return image_rect.tostring()

    def scale(self, old_scale, new_x_scale=300):
        """Provide the new image dimensions based on a new *new_x_scale*
        value.  Generally used to downsample an original image.

        .. note::

            If *new_x_scale* is greater than the original X coordinate
            dimension then downsampling is disabled and the original
            image dimensions are restored.

        **Args:**
            *old_scale*: current image dimensions

        **Kwargs:**
            *new_x_scale*: new X (column) dimension

        **Returns:**
            Tuple structure representing the new ``(X, Y)`` dimensions.

        """
        log.info('Scaling (X, Y) %s to %d columns ...' % (old_scale,
                                                          new_x_scale))

        y_scale = None
        x_scale = new_x_scale

        if new_x_scale > old_scale[0]:
            log.warn('New X-scale %d greater than original: %d' %
                     (new_x_scale, old_scale[0]))
            x_scale = old_scale[0]
            y_scale = old_scale[1]
        else:
            scale_factor = old_scale[0] / float(new_x_scale)
            y_scale = int(old_scale[1] / scale_factor)
            log.info('New (X, Y) scale: (%d, %d)' % (x_scale, y_scale))

        return (x_scale, y_scale)

    @staticmethod
    def reconstruct_image(image_stream, dimensions):
        """Reconstruct a 1D stream to an image file.

        **Args:**
            *image_stream*: ``string`` type stream of raw binary
            floating point data that is a 1D representation of an
            image file.  This is typically the format that is stored
            in the Accumulo image library

            *dimensions*: tuple structure that represents the rows
            and columns of the 1D image.  This information is used
            to recreate the original 2D structure of the image

        **Returns:**
            A :mod:`Image` image in memory from pixel data provided
            by the *image_stream*

        """
        log.debug('Reconstructing image with dimensions "%s"' %
                  str(dimensions))

        return Image.frombuffer('L',
                                dimensions,
                                image_stream(),
                                'raw',
                                'L', 0, 1)

    @staticmethod
    def reconstruct_mb_image(image_stream, dimensions):
        """Reconstruct a multi-band image from a 1D stream.

        **Args:**
            *image_stream*: ``string`` type stream of raw binary
            floating point data that is a 1D representation of an
            image file.  This is typically the format that is stored
            in the Accumulo image library

            *dimensions*: tuple structure that represents the rows
            and columns of the 1D image.  This information is used
            to recreate the original #D structure of the image.  For a
            RGB image this is typically (x_size, y_size, 3)

        **Returns:**
            A :mod:`Image` image in memory from pixel data provided
            by the *image_stream*

        """
        def reconstruct():
            arr = numpy.fromstring(image_stream(), dtype=numpy.uint8)
            arr = numpy.reshape(arr, dimensions)

            return Image.fromarray(arr)

        return reconstruct
