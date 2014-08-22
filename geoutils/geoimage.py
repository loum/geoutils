# pylint: disable=R0201
"""The :class:`geoutils.Image` class is a component of a
:class:`geoutils.Standard`: each standard presents a metadata component
and an image.  This class focuses on the image.

"""
__all__ = ["GeoImage"]

import Image

from oct.utils.log import log


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

        **Returns:**
            On success, type ``string`` that contains
            :func:`osgeo.gdal.Dataset.GetRasterBand.XSize` x 4 bytes
            of raw binary floating point data.

            ``None`` otherwise.

        """
        def generate():
            """TODO

            """
            log.info('Opening image stream ...')

            if dataset is None:
                log.warn('Extraction failed: dataset stream not provided')
                return
            else:
                band = dataset.GetRasterBand(1)
                x_size = band.XSize
                y_size = band.YSize
                if downsample is not None:
                    log.info('Downsample image to %d columns' %
                             downsample)
                    (x_size, y_size) = self.scale((band.XSize,
                                                   band.YSize),
                                                  downsample)

                return band.ReadRaster(0, 0,
                                       band.XSize,
                                       band.YSize,
                                       buf_xsize=x_size,
                                       buf_ysize=y_size)

        return generate

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
            log.debug('New (X, Y) scale: (%d, %d)' % (x_scale, y_scale))

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
