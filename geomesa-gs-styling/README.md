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

The geomesaLabelParser function handles lookup, parsing and formatting of labels. The first is the format for the values 
if they are parsable as a Double. Use standard Java number format syntax. The other three parameters are the values to 
parse. This can be any function or literal that will provide a value for the label. If the value of the label is not 
parsable as a Double the value is passed through as a string. This function always returns a string.

```
<Label>
    <ogc:Function name="geomesaParseLabel">
        <ogc:Literal>%.0f</ogc:Literal>
        <ogc:Function name="property">labelAttr1</ogc:Function>
        <ogc:Literal></ogc:Literal>
        <ogc:Literal></ogc:Literal>
    </ogc:Function>
    ...
```

```
<Label>
    <ogc:Function name="geomesaParseLabel">
        <ogc:Literal>%.4f</ogc:Literal>
        <ogc:Function name="property">
            <ogc:Function name="env">
                <ogc:Literal>label1</ogc:Literal>
                <ogc:Literal>name</ogc:Literal>
            </ogc:Function>
        </ogc:Function>
        <ogc:Function name="property">
            <ogc:Function name="env">
                <ogc:Literal>label2</ogc:Literal>
                <ogc:Literal>location</ogc:Literal>
            </ogc:Function>
        </ogc:Function>
        <ogc:Function name="property">
            <ogc:Function name="env">
                <ogc:Literal>label3</ogc:Literal>
                <ogc:Literal>age</ogc:Literal>
            </ogc:Function>
        </ogc:Function>
    </ogc:Function>
    ...
```

> :information_source: This project is not associated with Eclipse and/or Locationtech. It uses and/or links to GPL licensed code.
