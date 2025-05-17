from dataclasses import dataclass
from serializer import frames_pb2
import numpy as np
from typing import Literal

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

@dataclass
class Frame:
    c_time: np.datetime64
    a_time: np.datetime64
    data: np.ndarray

    @classmethod
    def from_protobuf(self, pb: frames_pb2.Frame) -> "Frame":
        dtype = bpw_to_dtype(pb.bits_per_word)
        if 
        data = np.frombuffer(pb.data, dtype=dtype)
        return Frame(
            c_time=np.datetime64(pb.c_time, "ns"),
            a_time=np.datetime64(pb.a_time, "ns"),
            data=data,
        )
