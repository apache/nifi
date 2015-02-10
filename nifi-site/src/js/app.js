// Foundation JavaScript
// Documentation can be found at: http://foundation.zurb.com/docs
$(document).foundation('topbar', {
    mobile_show_parent_link: false,
    is_hover: false
}).foundation('reveal', {
    animation: 'none'
});

// load fonts
$(document).ready(function() {
    WebFont.load({
        google: {
            families: ['Oswald:400,700,300']
        }
    });
});