#include <Keyboard.h>

const int buttonPin = 0;     // the number of the pushbutton pin

// variable for storing the pushbutton status 
int buttonState = 0;

void setup() {
  Serial.begin(9600);  
  // initialize the pushbutton pin as an input
  pinMode(buttonPin, INPUT_PULLDOWN);
    
  Keyboard.begin();
  delay(2000);
}

void loop() {
  // read the state of the pushbutton value
  buttonState = digitalRead(buttonPin);
  // check if the pushbutton is pressed.
  // if it is, the buttonState is HIGH
  if (buttonState == HIGH) {
    Serial.println("Button pressed");
    Keyboard.press(KEY_PAGE_DOWN);
    delay(100);
    Keyboard.releaseAll();
    delay(150);
  }
}
