# PROTOCOL, OFFERING_URL, METHOD, CODE, TYPE, HREF, CARDINALITY

# CARDINALITY is related to the occurrences of the same protocol + method:
# 1 = One and only one occurrence MUST be present
# 01 = Zero or one occurrences
# 0+ = Zero or more occurrences

# Takes into account only the listed protocols
protocols = ["WMS","WMTS","WCS"]

links = [
["WMS","http://www.opengis.net/spec/owc/1.0/req/atom/wms", "GET", "GetCapabilities", "application/xml", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/ows?service=wms&version=1.3.0&request=GetCapabilities", "1"],
["WMS","http://www.opengis.net/spec/owc/1.0/req/atom/wms", "GET", "GetMap", "image/jpeg", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/ows?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&FORMAT=image%2Fjpeg&STYLES&LAYERS={{WORKSPACE}}%3A{{LAYER}}&SRS=EPSG%3A4326&WIDTH={{WIDTH}}&HEIGHT={{HEIGHT}}&BBOX={{MINX}}%2C{{MINY}}%2C{{MAXX}}%2C{{MAXY}}", "0+"],

["WMTS", "http://www.opengis.net/spec/owc/1.0/req/atom/wmts", "GET", "GetCapabilities", "application/xml", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/gwc/service/wmts?REQUEST=GetCapabilities", "1"],
["WMTS", "http://www.opengis.net/spec/owc/1.0/req/atom/wmts", "GET", "GetTile", "image/jpeg", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/gwc/service/wmts?layer=sentinel2%3ATCI&style&tilematrixset=EPSG%3A4326&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix=EPSG%3A4326%3A13&TileCol=7780&TileRow=2248", "0+"],

["WCS", "http://www.opengis.net/spec/owc/1.0/req/atom/wcs", "GET", "GetCapabilities", "application/xml", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/ows?service=WCS&version=2.0.1&request=GetCapabilities", "1"],
#["WCS", "http://www.opengis.net/spec/owc/1.0/req/atom/wcs", "GET", "DescribeCoverage", "application/xml", "http://...", "0+"],
#["WCS", "http://www.opengis.net/spec/owc/1.0/req/atom/wcs", "GET", "DescribeEOCoverageSet", "application/xml", "http://...", "01"],
["WCS", "http://www.opengis.net/spec/owc/1.0/req/atom/wcs", "GET", "GetCoverage", "image/tiff", "${BASE_URL}/{{WORKSPACE}}/{{LAYER}}/{{PRODUCT_ID}}/ows?service=WCS&version=2.0.1&request=GetCoverage&coverageid={{WORKSPACE}}__{{LAYER}}&format=image%2Fgeotiff&scaleaxes=i({{WCS_SCALEFACTOR}}),j({{WCS_SCALEFACTOR}})&subset=Long({{MINX}}%2C{{MAXX}})&subset=Lat({{MINY}}%2C{{MAXY}})", "0+"],

["WFS", "http://www.opengis.net/spec/owc/1.0/req/atom/wfs", "GET", "GetCapabilities", "application/xml", "http://...", "1"],
["WFS", "http://www.opengis.net/spec/owc/1.0/req/atom/wfs", "GET", "DescribeFeature", "application/xml", "http://...", "0+"],
["WFS", "http://www.opengis.net/spec/owc/1.0/req/atom/wfs", "GET", "DescribeFeature", "shape-zip", "http://...", "0+"],

["WPS", "http://www.opengis.net/spec/owc/1.0/req/atom/wps", "GET", "GetCapabilities", "application/xml", "http://...","1"],
#["WPS", "http://www.opengis.net/spec/owc/1.0/req/atom/wps", "GET", "DescribeFeature", "application/xml", "http://...", "01"],
["WPS", "http://www.opengis.net/spec/owc/1.0/req/atom/wps", "GET", "Execute", "", "http://...", "1"]
]
