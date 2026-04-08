from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {
        "UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }

def machines_mapping(code):
    return machine_codes()[code]

def properties_codes():
    return {
        "A7": "LITERS",
        "W8": "QUALITY",
        "L1": "LIGHT",
        "T3": "TIME",
        "P6": "POWER",
        "G8": "GRADES"
    }

def properties_mapping(code):
    return properties_codes()[code]

def attributes_codes():    #claves de raw event a claves de rich event
    return {
        "TS": "TIMESTAMP",
        "MC": "MACHINE",
        "PR": "PRODUCT",
        "PS": "PROPS"
    }

def attributes_mapping(code):
    return attributes_codes()[code]

#def aux_a(event):
    #print(f"a: {event}")

#def aux_b(event):
    #print(f"b: {event}")

def aux(e):
    #pasar de raw a rich
    e = {attributes_mapping(k): v for k, v in e.items()}
    #codigos de propiedades a nombres
    e["PS"] = {properties_mapping(k): v for k, v in e["PS"].items()}
    #codigos de maquinas a nombres
    e["MACHINE"] = machine_codes()[e["MACHINE"]]
    print(e)
    return e


def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(

        """ cosas que hemos visto en clase, como map, filter, do_action... y las funciones que hemos definido para transformar los eventos:

        # ops.do_action(lambda e: print(f"event received:e")),
        # ops.map(lambda e: {attributes_mapping(k): e[k] for k in e.keys()}),
        # ops.map(lambda e:{**e, "MACHINE": machine_codes()[e["MACHINE"]]}),
        # ops.map(lambda e:{**e, "PROPS": {properties_mapping(k): v for k, v in e["PROPS"].items()}}),
        """

        #empieza con un print del evento recibido
        ops.do_action(lambda e: print(f"event received: {e}")),

        #filtro para quedarnos solo con eventos de máquinas que nos interesan
        ops.filter(lambda e: e["MC"] in machine_codes().keys()),
        ops.do_action(save_raw_event),
        ops.do_action(lambda e: print(f"raw event saved: {e}")),

        #transformación de eventos de raw a rich
        ops.map(aux),
        #ops.do_action(aux),
        ops.do_action(lambda e: print(f"event processed: {e}")),
        ops.do_action(send_rich_event),
        ops.do_action(save_rich_event)

        """ después de cada transformación, un print del evento resultante, para poder seguir el proceso de transformación y asegurarnos de que se están aplicando correctamente las funciones de transformación."""
    )