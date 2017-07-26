# geomesa-gs-styling
Custom sld functions for rendering marks and styles in GeoServer

## geomesaFastMark

The geomesaFastMark function caches icon rotations in order to save time while rendering marks by only calculating mark transformations once.
To use this function, replace the "WellKnownName" for your mark with the icon path and "PropertyName" that you use for your icon heading.

```
<Mark>
  <WellKnownName>
    <ogc:Function name="geomesaFastMark">
      <ogc:Literal>ttf://SansSerif.bold#0x2191</ogc:Literal>
      <ogc:PropertyName>iconHeading</ogc:PropertyName>
    </ogc:Function>
  </WellKnownName>
  ...
```

## geomesaLabelParser

The geomesaLabelParser function handles lookup, parsing and formatting of labels. The first parameter is the value to parse. This can
be any function or literal that will provide a value for the label. The second parameter is the format for the value if it is parsable
as a Double. Use standard Java number format syntax. If the value of the label is not parsable as a Double the value is passed through
as a string. This function always returns a string.

```
<Label>
    <ogc:Function name="geomesaParseLabel">
        <ogc:Function name="property">labelAttr</ogc:Function>
        <ogc:Literal>%.0f</ogc:Literal>
    </ogc:Function>
    ...
```

```
<Label>
    <ogc:Function name="geomesaParseLabel">
        <ogc:Function name="property">
            <ogc:Function name="env">
                <ogc:Literal>label</ogc:Literal>
                <ogc:Literal>name</ogc:Literal>
            </ogc:Function>
        </ogc:Function>
        <ogc:Literal>%.4f</ogc:Literal>
    </ogc:Function>
    ...
```

Equivalent SLD parsing code:

```
<Label>
    <ogc:Function name="if_then_else">
        <!-- IF -->
        <ogc:Function name="equalTo">
            <ogc:Function name="parseInt">
                <ogc:Function name="property">
                    <ogc:Function name="env">
                        <ogc:Literal>label1</ogc:Literal>
                        <ogc:Literal>label</ogc:Literal>
                    </ogc:Function>
                </ogc:Function>
            </ogc:Function>
            <ogc:Literal>0</ogc:Literal>
        </ogc:Function>
        <!-- THEN -->
        <ogc:Function name="property">
            <ogc:Function name="env">
                <ogc:Literal>label1</ogc:Literal>
                <ogc:Literal>label</ogc:Literal>
            </ogc:Function>
        </ogc:Function>
        <!-- ELSE -->
        <ogc:Function name="numberFormat">
            <ogc:Literal>#.#####</ogc:Literal>
            <ogc:Function name="property">
                <ogc:Function name="env">
                    <ogc:Literal>label1</ogc:Literal>
                    <ogc:Literal>label</ogc:Literal>
                </ogc:Function>
            </ogc:Function>
        </ogc:Function>
    </ogc:Function>
    ...
```

> :information_source: This project is not associated with Eclipse and/or Locationtech. It uses and/or links to GPL licensed code.
