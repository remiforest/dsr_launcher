
var STATUS_TIMER = 5000

$("#new_container_button").click(function(){
    $("#new_container_button").text("Starting new environment ...");
    $("#new_container_button").attr("disabled", true);
    $.ajax({
        url: 'new_container',
        type: 'post',
        success:function(data){
            console.log(data);
            window.location.reload();
        }
    });
})


$(".stop_container").click(function(){
    var button = $(this);
    button.text("Stopping ...");
    button.attr("disabled", true);
    $.ajax({
        url: 'stop_container',
        type: 'post',
        data: {"container":$(this).data("container")},
        success:function(data){
            console.log(data);
            window.location.reload();      
        }
    });
})


$(".delete_container").click(function(){
    var button = $(this);
    button.attr("disabled", true);
    $.ajax({
        url: 'delete_container',
        type: 'post',
        data: {"container":button.data("container")},
        success:function(data){
            console.log(data);
            window.location.reload();
        }
    });
})

$(".start_container").click(function(){
    var button = $(this);
    button.text("Deleting ...");
    button.attr("disabled", true);
    $.ajax({
        url: 'start_container',
        type: 'post',
        data: {"container":button.data("container")},
        success:function(data){
            console.log(data);
            window.location.reload();
        }
    });
})


function check_status(){
    $(".container_status").each(function(){
        var status = $(this);
        $.ajax({
            url: 'check_status',
            type: 'post',
            data: {"container":$(this).data("container")},
            success:function(data){
                console.log(data);
                status.text(data);
            }
        });
    });
    setTimeout(function(){
    check_status();
    }, STATUS_TIMER);
}


$( document ).ready(function() {
    var hostname = window.location.hostname;
    $(".container_link").each(function(){
        $(this).attr("href", "https://" + hostname + ":" + $(this).data("port"));
    })
    check_status();
})