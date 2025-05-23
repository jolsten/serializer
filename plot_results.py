import matplotlib.pyplot as plt
import pandas as pd


def main():
    df = pd.read_csv("results.csv")

    plt.figure(figsize=(12, 8))

    for name, group in df.groupby("name"):
        plt.plot(group["frame_size"], group["packed_size"], marker="o", label=name)

    plt.xlabel("Frame Size")
    plt.ylabel("Packed Size")
    plt.title("Packed Size vs Frame Size by Format")
    plt.legend(title="Format")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 8))
    for name, group in df.groupby("name"):
        plt.plot(
            group["frame_size"], group["roundtrip_time_ns"], marker="o", label=name
        )

    plt.xlabel("Frame Size")
    plt.ylabel("Roundtrip Time (ns)")
    plt.title("Roundtrip Time vs Frame Size by Format")
    plt.legend(title="Format")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
