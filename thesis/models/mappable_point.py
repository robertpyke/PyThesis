from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION
import logging
import datetime
import json
import csv
import os

from sqlalchemy import (
    Column,
    Integer,
    Text,
    Unicode,
    ForeignKey,
    )

from sqlalchemy.sql import expression

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref,
    )

from sqlalchemy import func as func
from geoalchemy2 import *
from geoalchemy2.functions import GenericFunction
import geoalchemy2.functions as geo_func

class ST_Collect(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Collect'
    type = types.Geometry

class ST_Multi(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Multi'
    type = types.Geometry

class MappablePoint(Base):
    __tablename__ = 'mappable_points'
    id = Column(Integer, primary_key=True)

    # The mappable point MUST be associated with a layer
    layer_id = Column(Integer, ForeignKey("layers.id"), nullable=False)
    layer = relationship("Layer", backref=backref("mappable_points", order_by=id, enable_typechecks=False))
    location = Column(Geometry(geometry_type='POINT', srid=DEFAULT_PROJECTION))

    def __init__(self, location_wkt, projection=DEFAULT_PROJECTION):
        """ Mappable Point constructor

            Takes the following params:
                * location_wkt : A WKT description of the mappable point
                * projection : The EPSG projection as an integer
        """
        self.location = WKTElement(location_wkt, srid=projection)

    @classmethod
    def uniq_list(class_, seq):
        seen = set()
        seen_add = seen.add
        return [ x for x in seq if x not in seen and not seen_add(x)]

    @classmethod
    def write_csvs(class_, results_dir, in_rows):
        class_.write_pre_process_csv(results_dir, in_rows)

        class_.write_get_points_as_geojson_csv(results_dir, in_rows)
        class_.write_get_points_as_wkt_csv(results_dir, in_rows)

        class_.write_get_points_as_geojson_str_csv(results_dir, in_rows)
        class_.write_get_points_as_wkt_str_csv(results_dir, in_rows)

    @classmethod
    def pre_process(class_, layer, **kwargs):
        pass

    @classmethod
    def test_pre_process(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: pre_process")

        start_t = datetime.datetime.now()

        result = class_.pre_process(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: pre_process")

        log.info(
            "(%s) pre_process(%s, %s) took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            delta_t_s,
        )

        return ["pre_process", class_.__name__, layer.name, delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def write_pre_process_csv(class_, results_dir, in_rows):
        rows =  [row for row in in_rows if row[0] == "pre_process"]

        out_rows = []
        layer_name_row = ["Strategy"]

        layer_names = []

        strategies = {}

        for row in rows:
            strategy   = row[1]
            layer_name = row[2]
            delta_t_s  = row[3]

            if strategy not in strategies:
                strategies[strategy] = {}
            strategies[strategy][layer_name] = delta_t_s

            layer_names.append(layer_name)

        uniq_layer_names = class_.uniq_list(layer_names)
        layer_names = sorted(uniq_layer_names)

        # Generate header
        for layer_name in layer_names:
            layer_name_row.append(layer_name)
        out_rows.append(layer_name_row)

        # Generate row for each strategy
        for strategy_name, strategy_dict in strategies.iteritems():
            row = [strategy_name]
            for layer_name in layer_names:
                delta_t_s = strategy_dict[layer_name]
                row.append(delta_t_s)
            out_rows.append(row)

        pre_process_file = os.path.join(results_dir, "pre_process.csv")
        # Write pre_process_file
        with open(pre_process_file, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(out_rows)

    @classmethod
    def get_points_as_geojson(class_, layer, **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsGeoJSON(
#                ST_Multi(ST_Collect(MappablePoint.location))
#            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).filter(
            MappablePoint.layer_id == layer.id
        ).group_by(
            MappablePoint.location
        )

        return q

    @classmethod
    def test_get_points_as_geojson(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_geojson")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_geojson(layer, **kwargs).all()

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_geojson")

        log.info(
            "(%s) get_points_as_geojson(%s, %s) clusters: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_geojson", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]


    @classmethod
    def write_get_points_as_geojson_csv(class_, results_dir, in_rows):
        rows =  [row for row in in_rows if row[0] == "get_points_as_geojson"]

        time_rows = []
        cluster_size_rows = []

        layer_name_row = ["Strategy", "BBOX"]

        layer_names = []
        bboxes = []

        strategies = {}

        for row in rows:
            strategy   = row[1]
            layer_name = row[2]
            clusters   = row[3]
            delta_t_s  = row[4]
            kwargs     = row[5]
            bbox       = str(kwargs["bbox"])

            if strategy not in strategies:
                strategies[strategy] = {}

            if layer_name not in strategies[strategy]:
                strategies[strategy][layer_name] = {}

            strategies[strategy][layer_name][bbox] = {
                "delta_t_s": delta_t_s,
                "clusters": clusters,
            }

            bboxes.append(bbox)
            layer_names.append(layer_name)

        uniq_layer_names = class_.uniq_list(layer_names)
        layer_names = sorted(uniq_layer_names)

        uniq_bboxes = class_.uniq_list(bboxes)
        bboxes = sorted(uniq_bboxes)

        # Generate header
        for layer_name in layer_names:
            layer_name_row.append(layer_name)

        # Add the header to the output CSVs
        time_rows.append(layer_name_row)
        cluster_size_rows.append(layer_name_row)

        # Generate row for each strategy, and each bbox
        for strategy_name, strategy_dict in strategies.iteritems():
            for bbox in bboxes:
                # Now write a row for each strategy/bbox/layer combo

                time_row = [strategy_name, bbox]
                cluster_size_row = [strategy_name, bbox]

                # 1st, do time.
                for layer_name in layer_names:
                    details = strategy_dict[layer_name][bbox]

                    time_row.append(details["delta_t_s"])
                    cluster_size_row.append(details["clusters"])

                time_rows.append(time_row)
                cluster_size_rows.append(cluster_size_row)

        get_points_as_geojson_time_csv = os.path.join(results_dir, "get_points_as_geojson_time.csv")
        # Write time file
        with open(get_points_as_geojson_time_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(time_rows)

        get_points_as_geojson_cluster_size_csv = os.path.join(results_dir, "get_points_as_geojson_cluster_size.csv")
        # Write cluster_size file
        with open(get_points_as_geojson_cluster_size_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(cluster_size_rows)

    @classmethod
    def get_points_as_geojson_str(class_, layer, **kwargs):
        q = class_.get_points_as_geojson(layer, **kwargs)
        result = q.all()

        return_geojson_obj = {
            "type": "FeatureCollection",
            "features": [
            ]
        }

        for el in result:
            centroid     = json.loads(el.centroid)
            cluster_size = el.cluster_size

            return_geojson_obj["features"].append({
                "type": "Feature",
                "geometry": centroid,
                "properties": {
                    "cluster_size": cluster_size
                }
            })

        return json.dumps(return_geojson_obj)

    @classmethod
    def test_get_points_as_geojson_str(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_geojson_str")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_geojson_str(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_geojson_str")

        log.info(
            "(%s) get_points_as_geojson_str(%s, %s) string length: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_geojson_str", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def write_get_points_as_geojson_str_csv(class_, results_dir, in_rows):
        rows =  [row for row in in_rows if row[0] == "get_points_as_geojson_str"]

        time_rows = []
        string_length_rows = []

        layer_name_row = ["Strategy", "BBOX"]

        layer_names = []
        bboxes = []

        strategies = {}

        for row in rows:
            strategy   = row[1]
            layer_name = row[2]
            string_length = row[3]
            delta_t_s  = row[4]
            kwargs     = row[5]
            bbox       = str(kwargs["bbox"])

            if strategy not in strategies:
                strategies[strategy] = {}

            if layer_name not in strategies[strategy]:
                strategies[strategy][layer_name] = {}

            strategies[strategy][layer_name][bbox] = {
                "delta_t_s": delta_t_s,
                "string_length": string_length,
            }

            bboxes.append(bbox)
            layer_names.append(layer_name)

        uniq_layer_names = class_.uniq_list(layer_names)
        layer_names = sorted(uniq_layer_names)

        uniq_bboxes = class_.uniq_list(bboxes)
        bboxes = sorted(uniq_bboxes)

        # Generate header
        for layer_name in layer_names:
            layer_name_row.append(layer_name)

        # Add the header to the output CSVs
        time_rows.append(layer_name_row)
        string_length_rows.append(layer_name_row)

        # Generate row for each strategy, and each bbox
        for strategy_name, strategy_dict in strategies.iteritems():
            for bbox in bboxes:
                # Now write a row for each strategy/bbox/layer combo

                time_row = [strategy_name, bbox]
                string_length_row = [strategy_name, bbox]

                for layer_name in layer_names:
                    details = strategy_dict[layer_name][bbox]

                    time_row.append(details["delta_t_s"])
                    string_length_row.append(details["string_length"])

                time_rows.append(time_row)
                string_length_rows.append(string_length_row)

        get_points_as_geojson_str_time_csv = os.path.join(results_dir, "get_points_as_geojson_str_time.csv")
        # Write time file
        with open(get_points_as_geojson_str_time_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(time_rows)

        get_points_as_geojson_str_string_length_csv = os.path.join(results_dir, "get_points_as_geojson_str_string_length.csv")
        # Write string_length file
        with open(get_points_as_geojson_str_string_length_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(string_length_rows)

    @classmethod
    def get_points_as_geojson_str(class_, layer, **kwargs):
        q = class_.get_points_as_geojson(layer, **kwargs)
        result = q.all()

        return_geojson_obj = {
            "type": "FeatureCollection",
            "features": [
            ]
        }

        for el in result:
            centroid     = json.loads(el.centroid)
            cluster_size = el.cluster_size

            return_geojson_obj["features"].append({
                "type": "Feature",
                "geometry": centroid,
                "properties": {
                    "cluster_size": cluster_size
                }
            })

        return json.dumps(return_geojson_obj)

    @classmethod
    def get_points_as_wkt(class_, layer, **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsText(
#                ST_Multi(ST_Collect(MappablePoint.location))
#            ).label("locations"),
            geo_func.ST_AsText(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).filter(
            MappablePoint.layer_id == layer.id
        ).group_by(
            MappablePoint.location
        )

        return q

    @classmethod
    def test_get_points_as_wkt(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_wkt")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_wkt(layer, **kwargs).all()

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_wkt")

        log.info(
            "(%s) get_points_as_wkt(%s, %s) clusters: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_wkt", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def write_get_points_as_wkt_csv(class_, results_dir, in_rows):
        rows =  [row for row in in_rows if row[0] == "get_points_as_wkt"]

        time_rows = []
        cluster_size_rows = []

        layer_name_row = ["Strategy", "BBOX"]

        layer_names = []
        bboxes = []

        strategies = {}

        for row in rows:
            strategy   = row[1]
            layer_name = row[2]
            clusters   = row[3]
            delta_t_s  = row[4]
            kwargs     = row[5]
            bbox       = str(kwargs["bbox"])

            if strategy not in strategies:
                strategies[strategy] = {}

            if layer_name not in strategies[strategy]:
                strategies[strategy][layer_name] = {}

            strategies[strategy][layer_name][bbox] = {
                "delta_t_s": delta_t_s,
                "clusters": clusters,
            }

            bboxes.append(bbox)
            layer_names.append(layer_name)

        uniq_layer_names = class_.uniq_list(layer_names)
        layer_names = sorted(uniq_layer_names)

        uniq_bboxes = class_.uniq_list(bboxes)
        bboxes = sorted(uniq_bboxes)

        # Generate header
        for layer_name in layer_names:
            layer_name_row.append(layer_name)

        # Add the header to the output CSVs
        time_rows.append(layer_name_row)
        cluster_size_rows.append(layer_name_row)

        # Generate row for each strategy, and each bbox
        for strategy_name, strategy_dict in strategies.iteritems():
            for bbox in bboxes:
                # Now write a row for each strategy/bbox/layer combo

                time_row = [strategy_name, bbox]
                cluster_size_row = [strategy_name, bbox]

                # 1st, do time.
                for layer_name in layer_names:
                    details = strategy_dict[layer_name][bbox]

                    time_row.append(details["delta_t_s"])
                    cluster_size_row.append(details["clusters"])

                time_rows.append(time_row)
                cluster_size_rows.append(cluster_size_row)

        get_points_as_wkt_time_csv = os.path.join(results_dir, "get_points_as_wkt_time.csv")
        # Write time file
        with open(get_points_as_wkt_time_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(time_rows)

        get_points_as_wkt_cluster_size_csv = os.path.join(results_dir, "get_points_as_wkt_cluster_size.csv")
        # Write cluster_size file
        with open(get_points_as_wkt_cluster_size_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(cluster_size_rows)

    @classmethod
    def get_points_as_wkt_str(class_, layer, **kwargs):
        q = class_.get_points_as_wkt(layer, **kwargs)
        result = q.all()

        wkt_centroids = []

        for el in result:
            centroid_wkt     = el.centroid

            wkt_centroids.append(centroid_wkt)

        return_str = "GEOMETRYCOLLECTION(" + ','.join(wkt_centroids) + ")"
        return return_str

    @classmethod
    def test_get_points_as_wkt_str(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_wkt_str")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_wkt_str(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_wkt_str")



        log.info(
            "(%s) get_points_as_wkt_str(%s, %s) string length: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_wkt_str", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def write_get_points_as_wkt_str_csv(class_, results_dir, in_rows):
        rows =  [row for row in in_rows if row[0] == "get_points_as_wkt_str"]

        time_rows = []
        string_length_rows = []

        layer_name_row = ["Strategy", "BBOX"]

        layer_names = []
        bboxes = []

        strategies = {}

        for row in rows:
            strategy   = row[1]
            layer_name = row[2]
            string_length = row[3]
            delta_t_s  = row[4]
            kwargs     = row[5]
            bbox       = str(kwargs["bbox"])

            if strategy not in strategies:
                strategies[strategy] = {}

            if layer_name not in strategies[strategy]:
                strategies[strategy][layer_name] = {}

            strategies[strategy][layer_name][bbox] = {
                "delta_t_s": delta_t_s,
                "string_length": string_length,
            }

            bboxes.append(bbox)
            layer_names.append(layer_name)

        uniq_layer_names = class_.uniq_list(layer_names)
        layer_names = sorted(uniq_layer_names)

        uniq_bboxes = class_.uniq_list(bboxes)
        bboxes = sorted(uniq_bboxes)

        # Generate header
        for layer_name in layer_names:
            layer_name_row.append(layer_name)

        # Add the header to the output CSVs
        time_rows.append(layer_name_row)
        string_length_rows.append(layer_name_row)

        # Generate row for each strategy, and each bbox
        for strategy_name, strategy_dict in strategies.iteritems():
            for bbox in bboxes:
                # Now write a row for each strategy/bbox/layer combo

                time_row = [strategy_name, bbox]
                string_length_row = [strategy_name, bbox]

                for layer_name in layer_names:
                    details = strategy_dict[layer_name][bbox]

                    time_row.append(details["delta_t_s"])
                    string_length_row.append(details["string_length"])

                time_rows.append(time_row)
                string_length_rows.append(string_length_row)

        get_points_as_wkt_str_time_csv = os.path.join(results_dir, "get_points_as_wkt_str_time.csv")
        # Write time file
        with open(get_points_as_wkt_str_time_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(time_rows)

        get_points_as_wkt_str_string_length_csv = os.path.join(results_dir, "get_points_as_wkt_str_string_length.csv")
        # Write string_length file
        with open(get_points_as_wkt_str_string_length_csv, 'wb') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            my_writer.writerows(string_length_rows)

    @classmethod
    def extra_log_details(class_):
        return {}
