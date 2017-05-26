<?xml version="1.0" encoding="UTF-8"?>
<p:declare-step xmlns:p="http://www.w3.org/ns/xproc"
                xmlns:c="http://www.w3.org/ns/xproc-step"
                name="multipleOutputs"
                version="1.0">

    <p:input port="source" />
    <p:output port="a">
        <p:pipe step="filtera" port="result" />
    </p:output>
    <p:output port="b" sequence="true">
        <p:pipe step="filterb" port="result" />
    </p:output>

    <p:filter select="/test/a" name="filtera">
        <p:input port="source">
            <p:pipe step="multipleOutputs" port="source" />
        </p:input>
    </p:filter>

    <p:filter select="/test/b" name="filterb">
        <p:input port="source">
            <p:pipe step="multipleOutputs" port="source" />
        </p:input>
    </p:filter>
 
</p:declare-step>
