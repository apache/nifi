<?xml version="1.0" encoding="UTF-8"?>
<p:pipeline xmlns:p="http://www.w3.org/ns/xproc"
            xmlns:c="http://www.w3.org/ns/xproc-step"
            name="test"
            version="1.0">

    <p:option name="attributeValA" required="true" />
    <p:option name="attributeValB" required="true" select="'default value'" /> <!-- causes error, cannot have required option with default value -->

    <p:add-attribute match="/test/a" attribute-name="testAttr">
        <p:with-option name="attribute-value" select="$attributeValA" />
    </p:add-attribute>

    <p:add-attribute match="/test/b" attribute-name="testAttr">
        <p:with-option name="attribute-value" select="$attributeValB" />
    </p:add-attribute>

</p:pipeline>
