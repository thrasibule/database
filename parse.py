import sys


def parse_variables(fh):
    d = {}
    for line in fh:
        line = line.strip()
        if line == ".":
            return d
        else:
            label, desc = line.split('"', 1)
            label = label.strip()
            desc = desc[:-1]
            d[label] = desc


def parse_values(fh):
    d = {}
    sent = True
    for line in fh:
        line = line.strip()
        if not line or line == ".":
            return d
        else:
            if sent or line[0] == "/":
                if sent:
                    variable = line
                else:
                    d[variable] = l
                    variable = line.split(" ", 1)[1]
                l = []
            else:  # accumulate values
                value, desc = line.split('"', 1)
                value = value.strip()
                desc = desc[:-1]
                l.append((value, desc))
        if sent:
            sent = False


def parse(filename):
    with open(filename) as fh:
        for line in fh:
            line = line.strip()
            if line == "VARIABLE LABELS":
                variables = parse_variables(fh)
            if line == "VALUE LABELS":
                values = parse_values(fh)
    return variables, values

if __name__ == "__main__":
    parse(sys.argv[1])
