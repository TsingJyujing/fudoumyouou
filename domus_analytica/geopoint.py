# Modified from https://github.com/TsingJyujing/GeoScala/blob/master/src/main/scala/com/github/tsingjyujing/geo/basic/IGeoPoint.scala
import math

from pydantic import BaseModel

EARTH_RADIUS = 6378.5
MAX_INNER_PRODUCT_FOR_UNIT_VECTOR = 1.0


class GeoPoint(BaseModel):
    latitude: float
    longitude: float

    def __sub__(self, other: "GeoPoint") -> float:
        dx = abs(self.longitude - other.longitude)
        dy = abs(self.latitude - other.latitude)

        if dx < 0.001 and dy < 0.001 and (self.latitude + other.latitude) < 120:
            return GeoPoint.local_euclid_distance(self, other)
        else:
            return GeoPoint.geodesic_distance(self, other)

    @staticmethod
    def local_euclid_distance(point1: "GeoPoint", point2: "GeoPoint") -> float:
        mean_latitude = (point1.latitude + point2.latitude) / 2.0
        delta_latitude = abs(point1.latitude - point2.latitude)
        delta_longitude = abs(point1.longitude - point2.longitude)
        dx = (
            EARTH_RADIUS
            * math.cos(math.radians(mean_latitude))
            * math.radians(delta_longitude)
        )
        dy = EARTH_RADIUS * math.radians(delta_latitude)
        return math.sqrt(dx**2 + dy**2)

    @staticmethod
    def geodesic_distance(point1: "GeoPoint", point2: "GeoPoint") -> float:
        alpha = GeoPoint.get_inner_product(point1, point2)
        if alpha >= MAX_INNER_PRODUCT_FOR_UNIT_VECTOR:
            return 0.0
        elif alpha <= -MAX_INNER_PRODUCT_FOR_UNIT_VECTOR:
            return EARTH_RADIUS * math.pi
        else:
            return math.acos(alpha) * EARTH_RADIUS

    @staticmethod
    def get_inner_product(point1: "GeoPoint", point2: "GeoPoint") -> float:
        return math.sin(math.radians(point1.latitude)) * math.sin(
            math.radians(point2.latitude)
        ) + math.cos(math.radians(point1.latitude)) * math.cos(
            math.radians(point2.latitude)
        ) * math.cos(
            math.radians(point1.longitude - point2.longitude)
        )

    @staticmethod
    def from_lat_lng(**kwargs) -> "GeoPoint":
        return GeoPoint(latitude=float(kwargs["lat"]), longitude=float(kwargs["lng"]))
