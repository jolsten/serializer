import timeit

import numpy as np
import pandas as pd

from serializer.frame import Frame


def time_json(frame: Frame):
    serialzied = frame.to_json()
    deserialized = Frame.from_json(serialzied)
    return deserialized


def time_json_b64(frame: Frame):
    serialzied = frame.to_json_b64()
    deserialized = Frame.from_json_b64(serialzied)
    return deserialized


def time_protobuf(frame: Frame):
    serialzied = frame.to_protobuf()
    deserialized = Frame.from_protobuf(serialzied)
    return deserialized


def time_msgpack(frame: Frame):
    serialzied = frame.to_msgpack()
    deserialized = Frame.from_msgpack(serialzied)
    return deserialized


def time_avro(frame: Frame):
    serialzied = frame.to_avro()
    deserialized = Frame.from_avro(serialzied)
    return deserialized


def time_avro_b64(frame: Frame):
    serialzied = frame.to_avro_b64()
    deserialized = Frame.from_avro_b64(serialzied)
    return deserialized


timers = {
    "json": time_json,
    "json_b64": time_json_b64,
    "protobuf": time_protobuf,
    "msgpack": time_msgpack,
    "avro": time_avro,
    "avro_b64": time_avro_b64,
}

RUN_COUNT = 1_000


def main():
    results = []
    c_time = np.datetime64("2020-01-01T00:00:00.000", "ns")
    a_time = np.datetime64("2020-01-01T00:00:01.000", "ns")

    for name, func in timers.items():
        for size in [16, 32, 64, 128, 256, 512, 1024]:
            row = {"name": name, "frame_size": size}

            data = np.arange(size, dtype="<u1")
            frame = Frame(c_time=c_time, a_time=a_time, data=data, bits_per_word=8)

            time = timeit.timeit(
                "timer(frame)",
                number=RUN_COUNT,
                globals={"frame": frame, "timer": func},
            )

            row["packed_size"] = len(getattr(frame, "to_" + name)())
            row["roundtrip_time_ns"] = int(time / RUN_COUNT * 1e9)

            results.append(row)

    df = pd.DataFrame(results)
    df.to_csv("results.csv")
    print(df)


if __name__ == "__main__":
    main()
