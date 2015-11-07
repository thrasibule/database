CREATE TABLE variables (
    variable VARCHAR(10),
    description VARCHAR(1000),
    PRIMARY KEY (variable)
);

CREATE TABLE vals (
    variable VARCHAR(10),
    value VARCHAR(10),
    description VARCHAR(1000),
    PRIMARY KEY(variable, value),
    FOREIGN KEY variable REFERENCES variables(variable)
);
