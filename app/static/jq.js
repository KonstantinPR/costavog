$(document).ready(function() {
    var $checks = $("input[type='checkbox']").change(function() {
        var checked = $checks.is(':checked');
        <!--                        $(".table-header").toggle(!checked);-->
        $(".edit").toggle(checked);
    });
    $checks.first().change();
});


window.addEventListener('scroll', function() {
    document.getElementById('showScroll').innerHTML = window.pageYOffset + 'px';
})