<?xml version="1.0" encoding="UTF-8"?>
<p:declare-step xmlns:p="http://www.w3.org/ns/xproc"
                xmlns:c="http://www.w3.org/ns/xproc-step"
                name="test"
                version="1.0">

    <p:input port="source" primary="false" />
    <p:output port="result" />


    <p:identity>
        <p:input port="source">
            <p:pipe step="test" port="source" />
        </p:input>
    </p:identity>

</p:declare-step>
