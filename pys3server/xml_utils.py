from xml.etree.ElementTree import Element

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
NS_URL = NS[1:-1]


def get_xml_attr(element: Element, name: str, get_all: bool = False, ns: str = NS) -> Element:
    path = f".//{ns}{name}"
    return element.findall(path) if get_all else element.find(path)
