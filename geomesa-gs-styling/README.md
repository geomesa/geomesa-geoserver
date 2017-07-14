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

> :information_source: This project is not associated with Eclipse and/or Locationtech. It uses and/or links to GPL licensed code.
