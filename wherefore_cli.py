
#from wherefore.KafkaMessageTracker import KafkaMessageTracker
import time
from curses_renderer import CursesRenderer


def main():
    #tracker = KafkaMessageTracker("dmsc-kafka01:9092", "jbi_heartbeat")
    #time.sleep(7)
    #a = tracker.get_latest_values()
    #for key in a:
    #    print(f"Type: {a[key].source_type}, Name: {a[key].source_name}")
    renderer = CursesRenderer()
    try:
        while True:
            renderer.draw()
            time.sleep(0.01)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
