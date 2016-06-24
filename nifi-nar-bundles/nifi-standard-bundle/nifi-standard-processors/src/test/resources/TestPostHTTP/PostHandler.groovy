package TestPostHTTP

import javax.servlet.http.HttpServletResponse

final contents = request.reader.text
if (!contents || contents.size() < 1){
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    return
}
print "Thank you for submitting flowfile content ${contents}"
