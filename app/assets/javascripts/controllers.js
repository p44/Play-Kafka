(function(angular) {
    'use strict';

    /** Controllers 1 */
    angular.module('playkafka.controllers', ['playkafka.services'])
        .controller('HomeCtrl', ['$routeParams', '$rootScope', '$scope', '$http',
            function ($routeParams, $rootScope, $scope, $http) {
                $scope.tick_feed_msgs = [];
                $scope.home_output = '';
                $scope.result_data_none = JSON.parse('{"msg":""}');
                $scope.result_data = $scope.result_data_none;

                $scope.displayErrorResult = function (data, status) {
                    $scope.home_output = 'Error ' + status + '.  ' + $scope.result_data.msg;
                };

                // PUT /tick - uses server current system time
                $scope.putTick = function () {
                    $scope.home_output = 'Please Wait...  ';
                    var url = '/tick';
                    $http({method: 'PUT', url: url
                    }).success(function (data) { // , status, headers, config
                        $scope.result_data = data;
                        $scope.home_output = $scope.result_data.msg;
                    }).error(function (data, status) { // , headers, config
                        console.log('PUT ' + url + ' ERROR ' + status);
                        $scope.displayErrorResult(data, status);
                    });
                };

                $scope.loadDefaults = function()  {
                    var defTick = JSON.parse('{"ts":0}');
                    $scope.tick_feed_msgs = [defTick, defTick, defTick, defTick, defTick, defTick, defTick, defTick, defTick, defTick];
                };

                /** handle incoming delivery feed messages: add to messages array */
                $scope.addTickFeedMsg = function (msg) {
                    var msgobj = JSON.parse(msg.data);
                    console.log('Recieved TickFeedMsg' + msg.data);
                    $scope.$apply(function () {
                        $scope.tick_feed_msgs.pop(); // take off last
                        $scope.tick_feed_msgs.unshift(msgobj); // add to first index of the array

                    });
                };

                /** start listening to the delivery feed for the ticks from kafka */
                $scope.listen = function () {
                    $scope.delivery_feed = new EventSource("/feed/tick");
                    $scope.delivery_feed.addEventListener("message", $scope.addTickFeedMsg, false);
                };

                $scope.loadDefaults(); // on page load fetch the latest 10 sightings
                $scope.listen(); // establish event source for sightings feed

            }]);


})(window.angular);