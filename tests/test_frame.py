import numpy as np

from serializer.frame import Frame


class TestRoundtrip:
    frame: Frame

    def setup_class(self):
        c_time = np.datetime64("2020-01-02T03:04:05.678", "ns")
        a_time = np.datetime64("2020-01-02T03:04:05.678", "ns")
        data = np.arange(256, dtype=">u1")
        self.frame = Frame(
            c_time=c_time,
            a_time=a_time,
            data=data,
            bits_per_word=8,
        )

    def _check(self, deserialized: Frame):
        assert self.frame.c_time == deserialized.c_time
        assert self.frame.a_time == deserialized.a_time
        assert self.frame.data.tolist() == deserialized.data.tolist()
        assert self.frame.bits_per_word == deserialized.bits_per_word
        assert self.frame.byte_order == deserialized.byte_order

    def test_json(self):
        serialized = self.frame.to_json()
        deserialized = Frame.from_json(serialized)
        assert isinstance(serialized, str)
        self._check(deserialized)

    def test_json_b64(self):
        serialized = self.frame.to_json_b64()
        deserialized = Frame.from_json_b64(serialized)
        assert isinstance(serialized, str)
        self._check(deserialized)

    def test_protobuf(self):
        serialized = self.frame.to_protobuf()
        deserialized = Frame.from_protobuf(serialized)
        assert isinstance(serialized, bytes)
        self._check(deserialized)

    def test_flatbuffers(self):
        serialized = self.frame.to_flatbuffers()
        deserialized = Frame.from_flatbuffers(serialized)
        assert isinstance(serialized, bytearray)
        self._check(deserialized)

    def test_msgpack(self):
        serialized = self.frame.to_msgpack()
        deserialized = Frame.from_msgpack(serialized)
        assert isinstance(serialized, bytes)
        self._check(deserialized)

    def test_avro(self):
        serialized = self.frame.to_avro()
        deserialized = Frame.from_avro(serialized)
        assert isinstance(serialized, bytes)
        self._check(deserialized)

    def test_avro_b64(self):
        serialized = self.frame.to_avro_b64()
        deserialized = Frame.from_avro_b64(serialized)
        assert isinstance(serialized, bytes)
        self._check(deserialized)
