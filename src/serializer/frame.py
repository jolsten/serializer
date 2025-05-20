import base64
import io
import json
import sys
from dataclasses import dataclass
from typing import Literal

import avro.schema
import msgpack
import numpy as np
import numpy.typing as npt
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

from serializer import frames_pb2


def bpw_to_dtype(value: int) -> Literal["u1", "u2", "u4", "u8"]:
    if value <= 8:
        return "u1"
    elif value <= 16:
        return "u2"
    elif value <= 32:
        return "u4"
    elif value <= 64:
        return "u8"
    raise ValueError


AVRO_SCHEMA = avro.schema.parse(open("frames.avsc").read())
avro_writer = DatumWriter(AVRO_SCHEMA)
avro_reader = DatumReader(AVRO_SCHEMA)

AVRO_SCHEMA_B64 = avro.schema.parse(open("frames_b64.avsc").read())
avro_writer_b64 = DatumWriter(AVRO_SCHEMA_B64)
avro_reader_b64 = DatumReader(AVRO_SCHEMA_B64)


@dataclass
class Frame:
    c_time: np.datetime64
    a_time: np.datetime64
    data: npt.NDArray
    bits_per_word: int

    @property
    def byte_order(self) -> Literal["LE", "BE"]:
        if self.data.dtype.byteorder == ">":
            return "BE"
        elif self.data.dtype.byteorder == "<":
            return "LE"
        elif self.data.dtype.byteorder == "|":
            return "LE"
        elif self.data.dtype.byteorder == "=":
            if sys.byteorder == "little":
                return "LE"
            return "BE"

    def to_dict(self) -> dict:
        return {
            "c_time": str(self.c_time),
            "a_time": str(self.a_time),
            "data": self.data.tolist(),
            # "data": base64.b64encode(self.data.tobytes()).decode(),
            "bits_per_word": self.bits_per_word,
            "byte_order": self.byte_order,
        }

    @classmethod
    def from_dict(cls, obj: dict) -> "Frame":
        c_time = np.datetime64(obj["c_time"], "ns")
        a_time = np.datetime64(obj["a_time"], "ns")
        dtype = bpw_to_dtype(obj["bits_per_word"])
        order = "<" if obj["byte_order"] == "lsbf" else ">"
        dtype = f"{order}{dtype}"
        # data = np.frombuffer(base64.b64decode(obj["data"]), dtype=dtype)
        data = np.array(obj["data"], dtype=dtype)
        return cls(
            c_time=c_time,
            a_time=a_time,
            data=data,
            bits_per_word=obj["bits_per_word"],
        )

    def to_dict_b64(self) -> dict:
        return {
            "c_time": str(self.c_time),
            "a_time": str(self.a_time),
            "data": base64.b64encode(self.data.tobytes()).decode(),
            "bits_per_word": self.bits_per_word,
            "byte_order": self.byte_order,
        }

    @classmethod
    def from_dict_b64(cls, obj: dict) -> "Frame":
        c_time = np.datetime64(obj["c_time"], "ns")
        a_time = np.datetime64(obj["a_time"], "ns")
        dtype = bpw_to_dtype(obj["bits_per_word"])
        order = "<" if obj["byte_order"] == "lsbf" else ">"
        dtype = f"{order}{dtype}"
        data = np.frombuffer(base64.b64decode(obj["data"]), dtype=dtype)
        # data = np.array(obj["data"], dtype=dtype)
        return cls(
            c_time=c_time,
            a_time=a_time,
            data=data,
            bits_per_word=obj["bits_per_word"],
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, text: str) -> "Frame":
        obj = json.loads(text)
        return cls.from_dict(obj)

    def to_json_b64(self) -> str:
        return json.dumps(self.to_dict_b64())

    @classmethod
    def from_json_b64(cls, text: str) -> "Frame":
        obj = json.loads(text)
        return cls.from_dict_b64(obj)

    def to_protobuf(self) -> bytes:
        pb = frames_pb2.Frame()
        pb.c_time = self.c_time.astype(np.int64).astype(int)
        pb.a_time = self.a_time.astype(np.int64).astype(int)
        pb.data = self.data.tobytes()
        pb.byte_order = self.byte_order
        pb.bits_per_word = self.bits_per_word
        return pb.SerializeToString()

    @classmethod
    def from_protobuf(cls, raw: bytes) -> "Frame":
        pb = frames_pb2.Frame()
        pb.ParseFromString(raw)
        dtype = bpw_to_dtype(pb.bits_per_word)
        order = "<" if pb.byte_order == "lsbf" else ">"
        dtype = f"{order}{dtype}"
        data = np.frombuffer(pb.data, dtype=dtype)
        return cls(
            c_time=np.datetime64(pb.c_time, "ns"),
            a_time=np.datetime64(pb.a_time, "ns"),
            data=data,
            bits_per_word=pb.bits_per_word,
        )

    def to_msgpack(self) -> bytes:
        obj = self.to_dict()
        return msgpack.packb(obj)

    @classmethod
    def from_msgpack(cls, raw: bytes) -> "Frame":
        obj = msgpack.unpackb(raw)
        return cls.from_dict(obj)

    def to_avro(self) -> bytes:
        buffer = io.BytesIO()
        encoder = BinaryEncoder(buffer)
        obj = {
            "c_time": str(self.c_time),
            "a_time": str(self.a_time),
            "data": self.data.tolist(),
            "bits_per_word": self.bits_per_word,
            "byte_order": self.byte_order,
        }
        avro_writer.write(obj, encoder)
        return buffer.getvalue()

    @classmethod
    def from_avro(cls, datum: bytes) -> "Frame":
        buffer = io.BytesIO(datum)
        decoder = BinaryDecoder(buffer)
        obj = avro_reader.read(decoder)
        dtype = bpw_to_dtype(obj["bits_per_word"])
        order = "<" if obj["byte_order"] == "lsbf" else ">"
        dtype = f"{order}{dtype}"
        data = np.array(obj["data"], dtype=dtype)
        return Frame(
            c_time=np.datetime64(obj["c_time"], "ns"),
            a_time=np.datetime64(obj["a_time"], "ns"),
            data=data,
            bits_per_word=obj["bits_per_word"],
        )

    def to_avro_b64(self) -> bytes:
        buffer = io.BytesIO()
        encoder = BinaryEncoder(buffer)
        obj = {
            "c_time": str(self.c_time),
            "a_time": str(self.a_time),
            "data": self.data.tobytes(),
            "bits_per_word": self.bits_per_word,
            "byte_order": self.byte_order,
        }
        avro_writer_b64.write(obj, encoder)
        return buffer.getvalue()

    @classmethod
    def from_avro_b64(cls, datum: bytes) -> "Frame":
        buffer = io.BytesIO(datum)
        decoder = BinaryDecoder(buffer)
        obj = avro_reader_b64.read(decoder)
        dtype = bpw_to_dtype(obj["bits_per_word"])
        order = "<" if obj["byte_order"] == "lsbf" else ">"
        dtype = f"{order}{dtype}"
        data = np.frombuffer(obj["data"], dtype=dtype)
        return Frame(
            c_time=np.datetime64(obj["c_time"], "ns"),
            a_time=np.datetime64(obj["a_time"], "ns"),
            data=data,
            bits_per_word=obj["bits_per_word"],
        )
